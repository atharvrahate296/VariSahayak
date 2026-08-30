package com.varisahayak.core.walkie

import android.Manifest
import android.content.Context
import android.content.pm.PackageManager
import android.util.Log
import androidx.core.content.ContextCompat
import com.varisahayak.BuildConfig
import com.varisahayak.core.common.DispatcherProvider
import com.varisahayak.core.di.ApplicationScope
import com.varisahayak.domain.repository.AuthRepository
import com.varisahayak.domain.repository.AuthState
import dagger.hilt.android.qualifiers.ApplicationContext
import io.github.jan.supabase.SupabaseClient
import io.github.jan.supabase.functions.functions
import io.ktor.client.call.body
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.contentType
import com.twilio.audioswitch.AudioDevice
import io.livekit.android.AudioOptions
import io.livekit.android.AudioType
import io.livekit.android.LiveKit
import io.livekit.android.LiveKitOverrides
import io.livekit.android.RoomOptions
import io.livekit.android.audio.AudioSwitchHandler
import io.livekit.android.events.RoomEvent
import io.livekit.android.events.collect
import io.livekit.android.room.Room
import io.livekit.android.room.participant.Participant
import io.livekit.android.room.track.LocalAudioTrackOptions
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.serialization.Serializable
import javax.inject.Inject
import javax.inject.Singleton
import kotlin.math.min

@Serializable
private data class TokenRequest(val room: String)

@Serializable
private data class TokenResponse(
    val ok: Boolean = false,
    val token: String? = null,
    val url: String? = null,
    val identity: String? = null,
    val name: String? = null,
    val message: String? = null,
)

/**
 * The real radio: push-to-talk over a self-hosted LiveKit server.
 *
 * ## How PTT is implemented
 *
 * The microphone track is published **once**, on join, and immediately muted. A press
 * unmutes it and a release mutes it again. It is never published and unpublished per press:
 * publishing renegotiates the peer connection, which takes hundreds of milliseconds, and a
 * radio that swallows the first word of every transmission is a radio nobody trusts.
 *
 * There is a sub-second window during join where the track is published before the mute
 * lands. `LocalParticipant.setMicrophoneEnabled` is the only public way to create the
 * default audio track, and it creates it unmuted. The window is bounded by two adjacent
 * suspending calls with nothing between them, and the widget reports Idle throughout.
 *
 * ## Overlap, deliberately
 *
 * There is no floor control. Every participant is subscribed to every other, and keying up
 * while somebody else is talking is allowed — if three people press at once you hear all
 * three. Blocking the second speaker would be the wrong failure in an emergency.
 *
 * ## Honesty
 *
 * [WalkieUiState.connection] is driven by the transport and nothing else. If the URL is
 * unset the widget reports NotConfigured; if the token call fails or the server is
 * unreachable it reports Disconnected. In both cases [WalkieUiState.canTransmit] is false
 * and the button reads "Radio unavailable". A PTT button that looks live and carries
 * nothing is the most dangerous thing this screen can do, so the transport's real state is
 * the only thing allowed to draw it.
 *
 * ## Known limit
 *
 * Foreground only. There is no foreground service, so Android will tear the connection
 * down some time after the app leaves the screen. Backgrounded operation is deliberately
 * out of scope for the demo; a real deployment needs a `microphone`-typed foreground
 * service with an ongoing notification.
 */
@Singleton
class LiveKitWalkieController @Inject constructor(
    @ApplicationContext private val context: Context,
    @ApplicationScope private val scope: CoroutineScope,
    private val supabase: SupabaseClient,
    private val authRepository: AuthRepository,
    private val dispatchers: DispatcherProvider,
) : WalkieController {

    private val _state = MutableStateFlow(
        WalkieUiState(
            channel = null,
            availableChannels = WalkieChannels.ALL,
            connection = if (SERVER_URL.isBlank()) {
                WalkieConnection.NotConfigured
            } else {
                WalkieConnection.Disconnected
            },
            // Never true here. This controller carries real audio or it reports that it
            // cannot; it does not pretend in either direction.
            isSimulated = false,
        ),
    )
    override val state: StateFlow<WalkieUiState> = _state.asStateFlow()

    /** The room for the channel currently joined. Touched only from [dispatchers].main. */
    private var room: Room? = null

    /** The connect-and-stay-connected loop for the current channel. */
    private var sessionJob: Job? = null

    private var levelJob: Job? = null
    private var autoUnkeyJob: Job? = null

    /**
     * What the *user* wants the mic to be, independent of what the SDK has caught up to.
     *
     * Press and release both arrive from a gesture handler and both need a suspending call
     * to act on. Recording the intent and letting one serialised applier chase it means a
     * fast key-tap-key cannot land the two calls out of order and leave the mic open.
     */
    private var desiredMicOpen = false
    private val micMutex = Mutex()

    init {
        if (SERVER_URL.isBlank()) {
            Log.w(TAG, "LIVEKIT_URL is not set; the radio will report itself unavailable.")
        } else {
            // The net follows the session, not the screen. Joining on sign-in means a
            // responder is reachable from the moment they are signed in, and navigating
            // between the map and an incident never drops them off the channel.
            scope.launch {
                authRepository.authState.distinctUntilChanged().collect { auth ->
                    when (auth) {
                        is AuthState.SignedIn ->
                            if (_state.value.channel == null) join(WalkieChannels.DEFAULT.id)

                        // NOT a sign-out. supabase-kt resets status to Initializing in
                        // onStop, so this arrives every time the app is backgrounded.
                        // Leaving on it would tear the channel down and rebuild it on every
                        // app switch — a reconnect, a fresh token, and several seconds of
                        // being off the net, for someone who never signed out.
                        AuthState.Unknown -> Unit

                        is AuthState.SignedOut, is AuthState.SessionExpired -> leave()
                    }
                }
            }
        }
    }

    // -----------------------------------------------------------------------------------
    // Channel lifecycle
    // -----------------------------------------------------------------------------------

    override fun join(channelId: String) {
        if (SERVER_URL.isBlank()) return

        val target = WalkieChannels.byId(channelId) ?: return
        val current = _state.value
        // Idempotent for the channel already being worked on, but NOT once that attempt has
        // given up: without the connection check a dropped channel could never be retried,
        // and the picker cannot re-select the chip that is already selected.
        if (current.channel?.id == target.id &&
            current.connection != WalkieConnection.Disconnected
        ) {
            return
        }

        val previous = sessionJob
        previous?.cancel()

        _state.update {
            it.copy(
                channel = target,
                connection = WalkieConnection.Connecting,
                isMicOpen = false,
                speakers = emptyList(),
                levels = emptyList(),
            )
        }

        sessionJob = scope.launch(dispatchers.main) {
            // Waited for, not merely cancelled. The previous session tears its room down in
            // a finally block, and both sessions share the `room` field — starting the new
            // connect before that teardown has run would have the old session disconnect
            // the *new* room, leaving the volunteer silently on no channel at all.
            previous?.cancelAndJoin()
            try {
                runSession(target)
            } finally {
                withContext(NonCancellable) { teardownRoom() }
            }
        }
    }

    override fun leave() {
        val previous = sessionJob
        previous?.cancel()
        stopTransmit()

        // The teardown coroutine becomes the new sessionJob rather than clearing the field.
        // The room is closed by the session's own finally block, and a join() arriving
        // straight after a leave() has to wait for that: clearing the field would give the
        // new session nothing to wait on, and it could take ownership of `room` while the
        // old teardown was still queued to null it out.
        sessionJob = scope.launch(dispatchers.main) { previous?.cancelAndJoin() }

        _state.update {
            it.copy(
                channel = null,
                // The roster stays: leaving a channel must still leave the volunteer a way
                // back onto one, and an emptied list would render an inert widget.
                connection = if (SERVER_URL.isBlank()) {
                    WalkieConnection.NotConfigured
                } else {
                    WalkieConnection.Disconnected
                },
                isMicOpen = false,
                speakers = emptyList(),
                levels = emptyList(),
            )
        }
    }

    /**
     * Connect, stay connected, and keep trying.
     *
     * The retry loop is not optional polish. The channel picker disables the chip that is
     * already selected, so a volunteer whose connection dropped has no control anywhere on
     * the widget that would reconnect them — without this they would be stranded on a dead
     * channel until they restarted the app.
     */
    private suspend fun runSession(channel: WalkieChannel) {
        var attempt = 0

        while (currentCoroutineContext().isActive) {
            _state.update { it.copy(connection = WalkieConnection.Connecting) }

            val ended = CompletableDeferred<Unit>()
            var connected = false

            try {
                val token = fetchToken(channel.id)

                val newRoom = LiveKit.create(
                    appContext = context.applicationContext,
                    options = RoomOptions(
                        // Audio only, so neither adaptive stream nor dynacast has anything
                        // to act on. Both left off to keep the negotiation minimal.
                        adaptiveStream = false,
                        dynacast = false,
                        // Defaults, stated rather than assumed: this is a phone held at
                        // arm's length in a crowd, and echo cancellation plus noise
                        // suppression are what make that intelligible at the far end.
                        audioTrackCaptureDefaults = LocalAudioTrackOptions(
                            noiseSuppression = true,
                            echoCancellation = true,
                            autoGainControl = true,
                            highPassFilter = true,
                            typingNoiseDetection = false,
                        ),
                    ),
                    overrides = LiveKitOverrides(audioOptions = radioAudioOptions()),
                )
                room = newRoom

                val events = scope.launch(dispatchers.main) {
                    newRoom.events.collect { event -> onRoomEvent(event, ended) }
                }

                try {
                    newRoom.connect(SERVER_URL, token)
                    publishMutedMicrophone(newRoom)
                    connected = true
                    attempt = 0

                    _state.update {
                        it.copy(
                            connection = WalkieConnection.Connected,
                            channel = it.channel?.copy(
                                memberCount = newRoom.remoteParticipants.size + 1,
                            ),
                        )
                    }

                    // Parks here until the room reports it is finished. Cancelling the
                    // session job cancels this await, which is how leave() gets out.
                    ended.await()
                } finally {
                    events.cancel()
                }
            } catch (cancellation: CancellationException) {
                throw cancellation
            } catch (error: Exception) {
                Log.w(TAG, "Radio session for ${channel.id} failed: ${error.javaClass.simpleName}: ${error.message}")
            }

            teardownRoom()
            _state.update {
                it.copy(
                    connection = WalkieConnection.Disconnected,
                    isMicOpen = false,
                    speakers = emptyList(),
                    levels = emptyList(),
                )
            }

            // A session that ran for a while and then dropped retries immediately; one that
            // never got up backs off, so an unreachable VM is not hammered once a second
            // for the length of the event.
            if (connected) {
                attempt = 0
            } else {
                attempt++
            }
            delay(backoffMs(attempt))
        }
    }

    private fun onRoomEvent(event: RoomEvent, ended: CompletableDeferred<Unit>) {
        when (event) {
            is RoomEvent.Connected ->
                _state.update { it.copy(connection = WalkieConnection.Connected) }

            is RoomEvent.Reconnecting ->
                _state.update { it.copy(connection = WalkieConnection.Connecting) }

            is RoomEvent.Reconnected ->
                _state.update { it.copy(connection = WalkieConnection.Connected) }

            is RoomEvent.Disconnected, is RoomEvent.FailedToConnect -> ended.complete(Unit)

            is RoomEvent.ActiveSpeakersChanged -> onActiveSpeakers(event.speakers)

            is RoomEvent.ParticipantConnected, is RoomEvent.ParticipantDisconnected ->
                _state.update {
                    it.copy(
                        channel = it.channel?.copy(
                            memberCount = event.room.remoteParticipants.size + 1,
                        ),
                    )
                }

            else -> Unit
        }
    }

    /**
     * Who is audible, loudest first.
     *
     * The local participant is filtered out. LiveKit reports it among the active speakers
     * while your own mic is open, and "Amit + 1 other" that silently counts yourself is a
     * lie about how many people are on the air.
     */
    private fun onActiveSpeakers(speakers: List<Participant>) {
        val localIdentity = room?.localParticipant?.identity

        val names = speakers
            .filter { it.identity != null && it.identity != localIdentity }
            .sortedByDescending { it.audioLevel }
            .map(::displayNameOf)
            .distinct()

        _state.update { it.copy(speakers = names) }
        syncLevelFeed()
    }

    private fun displayNameOf(participant: Participant): String =
        participant.name?.takeIf { it.isNotBlank() }
            ?: participant.identity?.value
            ?: UNKNOWN_SPEAKER

    private suspend fun teardownRoom() {
        val existing = room ?: return
        room = null
        runCatching { existing.disconnect() }
        runCatching { existing.release() }
        stopLevelFeed()
    }

    /**
     * Where the far end comes out.
     *
     * The one setting that decides whether this feature works at all: a radio whose
     * received audio goes to the earpiece is a radio nobody hears, because the phone is in
     * a hand or a vest pocket and not against an ear. The loudspeaker is named ahead of the
     * earpiece so the fallback when no headset is attached is the speaker, never the
     * receiver.
     *
     * A headset still wins when one is present — a volunteer who has plugged in or paired
     * one has said where they want the audio, and blasting the channel out of the
     * loudspeaker in a temple queue would be worse than useless.
     *
     * The list is stated rather than left to the SDK. It matches livekit-android 2.28's
     * default today, which is exactly why it is worth pinning: this is not a preference,
     * it is the difference between a working radio and a silent one, and it should not be
     * able to change underneath us on a dependency bump.
     *
     * [AudioType.CallAudioType] puts the device in MODE_IN_COMMUNICATION, so the volume
     * keys move the in-call stream and the hardware echo canceller is applied to the
     * capture — the same reason MODIFY_AUDIO_SETTINGS is in the manifest.
     */
    private fun radioAudioOptions(): AudioOptions = AudioOptions(
        audioOutputType = AudioType.CallAudioType(),
        audioHandler = AudioSwitchHandler(context.applicationContext).apply {
            preferredDeviceList = listOf(
                AudioDevice.BluetoothHeadset::class.java,
                AudioDevice.WiredHeadset::class.java,
                AudioDevice.Speakerphone::class.java,
                AudioDevice.Earpiece::class.java,
            )
        },
    )

    // -----------------------------------------------------------------------------------
    // Push to talk
    // -----------------------------------------------------------------------------------

    override fun startTransmit() {
        if (!_state.value.canTransmit) return

        // Backstop only — the shell asks for RECORD_AUDIO before the button becomes live.
        // Reached if the user revokes the permission from settings while the app is open.
        if (!hasMicrophonePermission()) {
            Log.w(TAG, "PTT pressed without RECORD_AUDIO; the mic stays closed.")
            return
        }

        desiredMicOpen = true
        applyMicrophoneState()
        _state.update { it.copy(isMicOpen = true) }
        syncLevelFeed()

        // The single highest-value safeguard in this feature. A phone in a pocket with the
        // mic open transmits fabric noise over everyone else for as long as it is there,
        // and on a channel with no floor control there is nothing anybody else can do about
        // it. Thirty seconds is longer than any real transmission and shorter than a walk.
        autoUnkeyJob?.cancel()
        autoUnkeyJob = scope.launch {
            delay(MAX_TRANSMIT_MS)
            Log.i(TAG, "Auto-unkeyed after ${MAX_TRANSMIT_MS}ms of continuous transmission.")
            stopTransmit()
        }
    }

    override fun stopTransmit() {
        autoUnkeyJob?.cancel()
        autoUnkeyJob = null

        desiredMicOpen = false
        applyMicrophoneState()
        _state.update { it.copy(isMicOpen = false) }
        syncLevelFeed()
    }

    /**
     * Chases [desiredMicOpen] with the SDK.
     *
     * The lock makes the two suspending calls mutually exclusive, and the desired flag is
     * re-read *inside* it, so a press-release pair that arrives faster than the SDK can
     * follow collapses to one call with the final value rather than racing to the wrong one.
     */
    private fun applyMicrophoneState() {
        scope.launch(dispatchers.main) {
            micMutex.withLock {
                val target = room ?: return@withLock
                val want = desiredMicOpen
                runCatching { target.localParticipant.setMicrophoneEnabled(want) }
                    .onFailure { Log.w(TAG, "setMicrophoneEnabled($want) failed: ${it.message}") }
            }
        }
    }

    /**
     * Publishes the microphone and mutes it in the same breath.
     *
     * Both calls, adjacent, on join — see the class comment for why the track is published
     * once rather than per press.
     */
    private suspend fun publishMutedMicrophone(target: Room) {
        if (!hasMicrophonePermission()) {
            // Joining without the permission is a supported state: you can hear the channel,
            // you just cannot key it. The track is published when the permission arrives and
            // the first press lands.
            Log.i(TAG, "Joined without RECORD_AUDIO; listening only until it is granted.")
            return
        }
        target.localParticipant.setMicrophoneEnabled(true)
        target.localParticipant.setMicrophoneEnabled(false)
        desiredMicOpen = false
    }

    private fun hasMicrophonePermission(): Boolean =
        ContextCompat.checkSelfPermission(context, Manifest.permission.RECORD_AUDIO) ==
            PackageManager.PERMISSION_GRANTED

    // -----------------------------------------------------------------------------------
    // Waveform
    // -----------------------------------------------------------------------------------

    /** Runs the level feed exactly while there is something to draw. */
    private fun syncLevelFeed() {
        if (_state.value.isActive) startLevelFeed() else stopLevelFeed()
    }

    /**
     * Real amplitude, from the participant who is actually talking.
     *
     * LiveKit reports `audioLevel` from server speaker updates, whose cadence is set by
     * `audio.update_interval` in livekit.yaml — coarser than this sample interval. The
     * exponential glide is display smoothing over genuine samples, not invented motion: if
     * the server reports nothing the bars fall to the floor and stay there, which is the
     * correct thing for a channel where nobody is speaking to look like.
     */
    private fun startLevelFeed() {
        if (levelJob?.isActive == true) return

        levelJob = scope.launch {
            val bars = ArrayDeque<Float>()
            var smoothed = 0f

            while (currentCoroutineContext().isActive) {
                val target = room
                val raw = when {
                    target == null -> 0f
                    _state.value.isMicOpen -> target.localParticipant.audioLevel
                    else -> target.activeSpeakers.maxOfOrNull { it.audioLevel } ?: 0f
                }

                smoothed += (raw.coerceIn(0f, 1f) - smoothed) * LEVEL_GLIDE
                bars.addLast(min(smoothed, 1f))
                while (bars.size > WAVEFORM_BAR_COUNT) bars.removeFirst()

                _state.update { it.copy(levels = bars.toList()) }
                delay(LEVEL_INTERVAL_MS)
            }
        }
    }

    private fun stopLevelFeed() {
        levelJob?.cancel()
        levelJob = null
        _state.update { it.copy(levels = emptyList()) }
    }

    // -----------------------------------------------------------------------------------
    // Token
    // -----------------------------------------------------------------------------------

    /**
     * Mints a join token through the `livekit-token` edge function.
     *
     * The LiveKit API secret is not in this APK and must never be. A signing key in a
     * release build is a public key, and anybody who extracts it can mint themselves a
     * token for the emergency channel. The function checks the caller's Supabase JWT,
     * stamps the token with their user id and display name, and keeps it short-lived.
     */
    private suspend fun fetchToken(channelId: String): String =
        withContext(dispatchers.io) {
            val response = supabase.functions.invoke("livekit-token") {
                contentType(ContentType.Application.Json)
                setBody(TokenRequest(room = channelId))
            }

            val body = response.body<TokenResponse>()
            val token = body.token
            check(body.ok && !token.isNullOrBlank()) {
                "livekit-token refused: ${body.message ?: "no token returned"}"
            }

            // The address deliberately comes from BuildConfig rather than from this
            // response. The debug build's cleartext exception names exactly one host, so
            // following a server-supplied address would either fail opaquely or, worse,
            // widen what the app is willing to talk to in plaintext.
            if (!body.url.isNullOrBlank() && body.url != SERVER_URL) {
                Log.w(TAG, "Server advertises ${body.url} but the build is pinned to $SERVER_URL.")
            }

            token
        }

    private companion object {
        const val TAG = "LiveKitWalkie"

        /** ws://<ip>:7880 from the git-ignored .env. Empty means "no radio configured". */
        val SERVER_URL: String = BuildConfig.LIVEKIT_URL.trim()

        const val MAX_TRANSMIT_MS = 30_000L
        const val LEVEL_INTERVAL_MS = 60L
        const val LEVEL_GLIDE = 0.35f
        const val UNKNOWN_SPEAKER = "Unknown"

        fun backoffMs(attempt: Int): Long = when {
            attempt <= 0 -> 500L
            attempt == 1 -> 2_000L
            attempt == 2 -> 5_000L
            else -> 10_000L
        }
    }
}
