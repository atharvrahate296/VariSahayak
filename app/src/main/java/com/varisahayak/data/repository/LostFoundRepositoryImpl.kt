package com.varisahayak.data.repository

import android.util.Log
import com.varisahayak.core.common.AppError
import com.varisahayak.core.common.Clock
import com.varisahayak.core.common.DispatcherProvider
import com.varisahayak.core.common.Outcome
import com.varisahayak.core.common.getOrNull
import com.varisahayak.core.media.FaceSignature
import com.varisahayak.core.media.PhotoCapture
import com.varisahayak.data.local.dao.CustodyDao
import com.varisahayak.data.local.dao.LostFoundDao
import com.varisahayak.data.local.dao.LostFoundMatchDao
import com.varisahayak.data.local.entity.CustodyEntity
import com.varisahayak.data.local.entity.LostFoundMatchEntity
import com.varisahayak.data.remote.dto.FaceProcessingDto
import com.varisahayak.data.remote.dto.LostFoundMatchDto
import com.varisahayak.data.remote.dto.LostFoundReportDto
import com.varisahayak.data.sync.SyncScheduler
import com.varisahayak.domain.model.CustodyRecord
import com.varisahayak.domain.model.FaceMatchStatus
import com.varisahayak.domain.model.GeoPoint
import com.varisahayak.domain.model.IncidentCategory
import com.varisahayak.domain.model.LostFoundKind
import com.varisahayak.domain.model.LostFoundMatch
import com.varisahayak.domain.model.LostFoundReport
import com.varisahayak.domain.model.LostFoundStatus
import com.varisahayak.domain.model.LostFoundSubjectType
import com.varisahayak.domain.model.MatchStatus
import com.varisahayak.domain.model.SyncState
import com.varisahayak.domain.repository.AttributeSearch
import com.varisahayak.domain.repository.IncidentRepository
import com.varisahayak.domain.repository.LostFoundRepository
import com.varisahayak.domain.repository.RewardRepository
import com.varisahayak.domain.repository.ReportDetails
import com.varisahayak.domain.usecase.LostFoundMatchingEngine
import io.github.jan.supabase.SupabaseClient
import io.github.jan.supabase.auth.auth
import io.github.jan.supabase.functions.functions
import io.github.jan.supabase.postgrest.from
import io.ktor.client.call.body
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.contentType
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.withContext
import kotlinx.serialization.Serializable
import android.util.Base64
import java.util.UUID
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
class LostFoundRepositoryImpl @Inject constructor(
    private val supabase: SupabaseClient,
    private val lostFoundDao: LostFoundDao,
    private val custodyDao: CustodyDao,
    private val matchDao: LostFoundMatchDao,
    private val incidentRepository: IncidentRepository,
    private val rewardRepository: RewardRepository,
    private val matchingEngine: LostFoundMatchingEngine,
    private val syncScheduler: SyncScheduler,
    private val dispatchers: DispatcherProvider,
    private val clock: Clock,
) : LostFoundRepository {

    override fun observeAll(): Flow<List<LostFoundReport>> =
        lostFoundDao.observeAll().map { rows -> rows.map { it.toDomain() } }

    override fun observeByKind(kind: LostFoundKind): Flow<List<LostFoundReport>> =
        lostFoundDao.observeByKind(kind.wireName).map { rows -> rows.map { it.toDomain() } }

    override fun observeActive(): Flow<List<LostFoundReport>> =
        lostFoundDao.observeActive().map { rows -> rows.map { it.toDomain() } }

    override fun observeById(clientId: String): Flow<LostFoundReport?> =
        lostFoundDao.observeByClientId(clientId).map { it?.toDomain() }

    override fun search(query: String): Flow<List<LostFoundReport>> =
        lostFoundDao.search(query).map { rows -> rows.map { it.toDomain() } }

    /**
     * Structured filtering, applied in memory over the active set.
     *
     * In memory rather than in SQL because every filter is optional and most are ranges —
     * expressing that as one query means a dozen `(:x IS NULL OR col = :x)` clauses that
     * defeat every index anyway. The active set is a few hundred rows on a route.
     */
    override fun searchStructured(criteria: AttributeSearch): Flow<List<LostFoundReport>> =
        lostFoundDao.observeAll().map { rows ->
            rows.map { it.toDomain() }.filter { report -> report.matches(criteria) }
        }

    override fun observeUnsyncedCount(): Flow<Int> = lostFoundDao.observeUnsyncedCount()

    // --- reporting ----------------------------------------------------------------------------

    override suspend fun report(details: ReportDetails): Outcome<LostFoundReport> =
        withContext(dispatchers.io) {
            val title = details.title.trim()
            if (title.isBlank()) {
                return@withContext Outcome.Failure(
                    AppError.Validation(
                        field = "title",
                        message = "Add a short description of who or what is missing.",
                    ),
                )
            }

            // Deliberately the *only* other validation. A photograph, a name, an age, a
            // location — every one of them is optional, because a parent who reaches a
            // volunteer at dusk with none of them must still be able to file something the
            // engine can match on.
            if (details.approximateAge != null && details.approximateAge !in 0..120) {
                return@withContext Outcome.Failure(
                    AppError.Validation(
                        field = "approximateAge",
                        message = "Enter an approximate age between 0 and 120.",
                    ),
                )
            }

            val reporterId = supabase.auth.currentUserOrNull()?.id
                ?: return@withContext Outcome.Failure(AppError.Unauthorised())

            val now = clock.nowEpochMillis()

            // A missing person is an emergency, not a filing task. The incident is raised
            // first so it enters prioritisation, matching and notification immediately; the
            // searchable record is attached to it. A failed incident does not lose the
            // report — it is still saved locally and retried.
            val incidentClientId = if (details.subjectType == LostFoundSubjectType.PERSON &&
                details.kind == LostFoundKind.LOST
            ) {
                (
                    incidentRepository.createIncident(
                        category = IncidentCategory.LOST_PERSON,
                        description = buildIncidentDescription(details),
                        location = details.lastKnownLocation ?: details.deviceLocation,
                        photoLocalPath = details.photoLocalPath,
                        affectedPersonNote = null,
                        isSos = false,
                        sosBridgeToken = details.qrLocationToken,
                    ) as? Outcome.Success
                    )?.data?.clientId
            } else {
                null
            }

            val report = LostFoundReport(
                clientId = UUID.randomUUID().toString(),
                incidentClientId = incidentClientId,
                kind = details.kind,
                subjectType = details.subjectType,
                title = title,
                description = details.description.trim(),
                personName = details.personName?.trim()?.ifBlank { null },
                approximateAge = details.approximateAge,
                gender = details.gender?.trim()?.ifBlank { null },
                approximateHeightCm = details.approximateHeightCm,
                clothingDescription = details.clothingDescription?.trim()?.ifBlank { null },
                physicalDescription = details.physicalDescription?.trim()?.ifBlank { null },
                language = details.language?.trim()?.ifBlank { null },
                condition = details.condition?.trim()?.ifBlank { null },
                additionalNotes = details.additionalNotes?.trim()?.ifBlank { null },
                guardianName = details.guardianName?.trim()?.ifBlank { null },
                guardianPhone = details.guardianPhone?.trim()?.ifBlank { null },
                qrLocationToken = details.qrLocationToken,
                qrLocationName = details.qrLocationName,
                deviceLocation = details.deviceLocation,
                lastKnownLocation = details.lastKnownLocation ?: details.deviceLocation,
                routeSegment = details.routeSegment,
                routeSequence = details.routeSequence,
                occurredAtEpochMillis = details.occurredAtEpochMillis ?: now,
                reportedAtEpochMillis = now,
                photoLocalPath = details.photoLocalPath,
                // PENDING when a photo exists: the server owns this field and decides
                // whether the image yields a usable embedding.
                faceMatchStatus = if (details.photoLocalPath != null) {
                    FaceMatchStatus.PENDING
                } else {
                    FaceMatchStatus.NOT_APPLICABLE
                },
                // Whoever files a Found report is holding the person until they say
                // otherwise. Recorded immediately so "who has this child" is never blank.
                custodianUserId = if (details.kind == LostFoundKind.FOUND) reporterId else null,
                status = LostFoundStatus.OPEN,
                reportedBy = reporterId,
                syncState = SyncState.PENDING,
            )

            lostFoundDao.upsert(report.toEntity())

            // Gamification: Award XP for filing a report
            rewardRepository.awardXp(
                amount = com.varisahayak.domain.model.RewardEngine.XP_PROMPT_RESPONSE,
                reason = "Filed ${report.kind.name.lowercase()} report",
                relatedEntityId = report.clientId
            )
            rewardRepository.recordImpact(lostFoundAssisted = 1)

            if (details.kind == LostFoundKind.FOUND) {
                recordCustodyInternal(
                    reportClientId = report.clientId,
                    custodianUserId = reporterId,
                    custodianName = null,
                    helpPointName = details.qrLocationName,
                    qrLocationToken = details.qrLocationToken,
                    location = details.deviceLocation,
                    handoverNote = null,
                    at = now,
                )
            }

            // Candidates immediately and locally, so a volunteer who has just filed sees
            // the other side of the board without waiting for a network round trip.
            runCatching { findCandidates(report.clientId) }
                .onFailure { Log.d(TAG, "Local candidate pass skipped: ${it.message}") }

            syncScheduler.requestSync()
            Outcome.Success(report)
        }

    /**
     * Uploads a report's photograph for face processing and records the verdict.
     *
     * Goes through the `process-face` Edge Function rather than calling the Python service
     * directly. That indirection is the security design: the function authorises the caller
     * under RLS before doing anything, and the face service's shared secret stays on the
     * server instead of being shipped inside an APK where anyone can extract it.
     *
     * Every failure is swallowed into a status. Nothing here can fail a report — the report
     * was saved before this ran and stays matchable on its other attributes either way.
     */
    override suspend fun submitPhotoForMatching(
        clientId: String,
    ): Outcome<FaceMatchStatus> = withContext(dispatchers.io) {
        val report = lostFoundDao.getByClientId(clientId)?.toDomain()
            ?: return@withContext Outcome.Failure(AppError.NotFound())

        val path = report.photoLocalPath
            ?: return@withContext Outcome.Success(FaceMatchStatus.NOT_APPLICABLE)

        // An umbrella has no face. Deciding it here rather than at the far end saves a
        // base64 upload and a container cold start, and NOT_APPLICABLE is exactly what the
        // board should show for a lost bag.
        if (report.subjectType != LostFoundSubjectType.PERSON) {
            recordFaceStatus(clientId, FaceMatchStatus.NOT_APPLICABLE)
            return@withContext Outcome.Success(FaceMatchStatus.NOT_APPLICABLE)
        }

        val bytes = PhotoCapture.readBytes(path)
        if (bytes == null || bytes.isEmpty()) {
            // The file went missing between filing and uploading. Recorded rather than
            // retried forever, so the report stops claiming a photo it no longer has.
            recordFaceStatus(clientId, FaceMatchStatus.INVALID_IMAGE)
            return@withContext Outcome.Success(FaceMatchStatus.INVALID_IMAGE)
        }

        // The on-device pass runs first, always, and never touches the network. It is what
        // makes this feature work on the route: no signal, no configured CV service, and a
        // volunteer still gets a face verdict and face-ranked candidates in about a second.
        // The server, when it is reachable, is an upgrade on this - not a prerequisite.
        val local = FaceSignature.of(path)
        var status = local.toFaceMatchStatus()

        val result = if (report.serverId == null) {
            // Not pushed yet. `process-face` looks the report up in PostgreSQL by client id
            // under the caller's RLS, so this call could only spend a full base64 upload to
            // earn a 404. The sync worker picks the report up again once the row exists.
            null
        } else try {
            // NO_WRAP: a base64 body with embedded newlines is not valid JSON string content
            // and the function would reject the whole request.
            val encoded = Base64.encodeToString(bytes, Base64.NO_WRAP)

            val response = supabase.functions.invoke("process-face") {
                contentType(ContentType.Application.Json)
                setBody(
                    FaceProcessingRequest(report_client_id = clientId, image = encoded),
                )
            }

            response.body<FaceProcessingDto>()
        } catch (cancellation: CancellationException) {
            throw cancellation
        } catch (error: Exception) {
            // Offline, timed out, function not deployed, a body that would not parse. All
            // the same to a volunteer, and none of them fatal any more: the on-device
            // verdict above already stands on its own. The message is logged, never shown -
            // it can carry a URL or a serialisation detail that means nothing to the person
            // holding the phone.
            Log.d(TAG, "Remote face processing unavailable: ${error.message}")
            null
        }

        // Facenet beats a local binary pattern descriptor, so a READY from the server is
        // taken over the local verdict. Anything else from the server only counts when this
        // device could not read a face itself - a service outage, or a stricter server-side
        // detector, must never retire a photograph this device successfully described.
        val remote = result?.let { FaceMatchStatus.fromWire(it.status) }
        if (remote == FaceMatchStatus.READY) {
            status = FaceMatchStatus.READY
        } else if (
            remote != null &&
            remote != FaceMatchStatus.SERVICE_UNAVAILABLE &&
            local !is FaceSignature.Result.Ready
        ) {
            status = remote
        }

        // SERVICE_UNAVAILABLE is deliberately not written. It is transient, and persisting it
        // would be indistinguishable from a genuinely unusable photograph on the next pass.
        if (status != FaceMatchStatus.SERVICE_UNAVAILABLE) {
            recordFaceStatus(clientId, status)
        }

        // The whole point of the round trip. A face distance exists for exactly this moment
        // - the server holds no per-pair result to fetch later - so the candidate pass runs
        // here, while the numbers are in hand. Server distances are passed in; rankAndStore
        // fills every remaining candidate from the on-device descriptors, so the engine
        // scores on face as well as the other nine attributes either way.
        runCatching { rankAndStore(clientId, result?.distanceTo.orEmpty()) }
            .onFailure { Log.d(TAG, "Face-weighted candidate pass skipped: ${it.message}") }

        Outcome.Success(status)
    }

    /**
     * Re-attempts face processing for every report still waiting on it.
     *
     * Two states qualify, and they are genuinely different failures with the same fix:
     * PENDING means the photograph never reached the service (filed offline, or the app was
     * killed before [submitPhotoForMatching] ran), SERVICE_UNAVAILABLE means it reached it
     * and the round trip failed. Neither is a bad photograph, and neither self-corrects.
     *
     * Unsynced rows are deliberately included. [submitPhotoForMatching] is local-first, so
     * a report filed offline gets its descriptor and its face-ranked candidates here without
     * ever reaching the network - and that report, filed by a volunteer with no signal, is
     * precisely the one that most needs them. The remote leg is skipped for it until the
     * sync worker has pushed the row.
     *
     * Two deliberate limits remain:
     *
     * 1. **Bounded per pass.** The face service runs at concurrency 1 with no warm
     *    instance, so a backlog dispatched at once would queue behind itself and blow the
     *    worker's time budget. A handful per pass drains steadily instead.
     * 2. **Sequential.** Same reason. Firing these in parallel makes every one of them
     *    slower on a service that handles one request at a time.
     */
    override suspend fun retryPendingFaceProcessing(): Outcome<Int> =
        withContext(dispatchers.io) {
            val awaiting = lostFoundDao.getAwaitingFaceProcessing()
                .take(MAX_FACE_RETRIES_PER_PASS)

            var settled = 0

            for (report in awaiting) {
                val status = submitPhotoForMatching(report.clientId).getOrNull()
                    ?: FaceMatchStatus.SERVICE_UNAVAILABLE

                // Still unavailable means the outage has not lifted. Stop rather than walk
                // the rest of the backlog into the same wall — the worker is already
                // scheduled to come back with backoff.
                if (status == FaceMatchStatus.SERVICE_UNAVAILABLE) break

                settled++
            }

            Outcome.Success(settled)
        }

    /**
     * Writes a face verdict locally without touching sync state.
     *
     * The server owns this column — it is set by the Edge Function on the row the sync
     * worker already pushed — so marking the row PENDING here would send the client's copy
     * back up and overwrite the authoritative value with itself.
     */
    private suspend fun recordFaceStatus(clientId: String, status: FaceMatchStatus) {
        val current = lostFoundDao.getByClientId(clientId)?.toDomain() ?: return
        lostFoundDao.upsert(current.copy(faceMatchStatus = status).toEntity())
    }

    override suspend fun update(
        clientId: String,
        mutate: (LostFoundReport) -> LostFoundReport,
    ): Outcome<LostFoundReport> = withContext(dispatchers.io) {
        val existing = lostFoundDao.getByClientId(clientId)?.toDomain()
            ?: return@withContext Outcome.Failure(AppError.NotFound())

        val updated = mutate(existing).let { next ->
            // Replacing the photo invalidates any prior face verdict: the new image has to
            // be processed before it can contribute a face signal again.
            if (next.photoLocalPath != existing.photoLocalPath && next.photoLocalPath != null) {
                // The descriptor cached beside the old file describes a different face.
                FaceSignature.forget(existing.photoLocalPath)
                next.copy(faceMatchStatus = FaceMatchStatus.PENDING)
            } else {
                next
            }
        }.copy(syncState = SyncState.PENDING)

        lostFoundDao.upsert(updated.toEntity())
        syncScheduler.requestSync()
        Outcome.Success(updated)
    }

    override suspend fun setStatus(clientId: String, status: LostFoundStatus): Outcome<Unit> =
        withContext(dispatchers.io) {
            lostFoundDao.getByClientId(clientId)
                ?: return@withContext Outcome.Failure(AppError.NotFound())

            lostFoundDao.setStatus(clientId, status.wireName)
            lostFoundDao.setSyncState(clientId, SyncState.PENDING.name)
            syncScheduler.requestSync()
            Outcome.Success(Unit)
        }

    // --- custody ------------------------------------------------------------------------------

    override fun observeCustodyChain(reportClientId: String): Flow<List<CustodyRecord>> =
        custodyDao.observeForReport(reportClientId).map { rows -> rows.map { it.toDomain() } }

    override suspend fun recordCustody(
        reportClientId: String,
        custodianUserId: String,
        custodianName: String?,
        helpPointName: String?,
        qrLocationToken: String?,
        location: GeoPoint?,
        handoverNote: String?,
    ): Outcome<CustodyRecord> = withContext(dispatchers.io) {
        lostFoundDao.getByClientId(reportClientId)
            ?: return@withContext Outcome.Failure(AppError.NotFound())

        val record = recordCustodyInternal(
            reportClientId = reportClientId,
            custodianUserId = custodianUserId,
            custodianName = custodianName,
            helpPointName = helpPointName,
            qrLocationToken = qrLocationToken,
            location = location,
            handoverNote = handoverNote,
            at = clock.nowEpochMillis(),
        )

        syncScheduler.requestSync()
        Outcome.Success(record.toDomain())
    }

    /**
     * Writes the custody span and mirrors the current holder onto the report.
     *
     * The mirror is denormalisation on purpose: the list and map views need "who has this
     * person" without joining a chain per row, and that question is asked constantly.
     */
    private suspend fun recordCustodyInternal(
        reportClientId: String,
        custodianUserId: String,
        custodianName: String?,
        helpPointName: String?,
        qrLocationToken: String?,
        location: GeoPoint?,
        handoverNote: String?,
        at: Long,
    ): CustodyEntity {
        val record = CustodyEntity(
            clientId = UUID.randomUUID().toString(),
            reportClientId = reportClientId,
            custodianUserId = custodianUserId,
            custodianName = custodianName,
            helpPointName = helpPointName,
            qrLocationToken = qrLocationToken,
            latitude = location?.latitude,
            longitude = location?.longitude,
            fromEpochMillis = at,
            untilEpochMillis = null,
            handoverNote = handoverNote,
            syncState = SyncState.PENDING.name,
        )

        custodyDao.handOver(record)
        lostFoundDao.setCustodian(reportClientId, custodianUserId, custodianName, null)
        lostFoundDao.setSyncState(reportClientId, SyncState.PENDING.name)

        return record
    }

    // --- matching -----------------------------------------------------------------------------

    override fun observeCandidateMatches(): Flow<List<LostFoundMatch>> =
        matchDao.observeCandidates().map { rows -> rows.map { it.toDomain() } }

    override fun observeMatchesForReport(reportClientId: String): Flow<List<LostFoundMatch>> =
        matchDao.observeForReport(reportClientId).map { rows -> rows.map { it.toDomain() } }

    override fun observeMatchById(clientId: String): Flow<LostFoundMatch?> =
        matchDao.observeByClientId(clientId).map { it?.toDomain() }

    override fun observeCandidateCount(): Flow<Int> = matchDao.observeCandidateCount()

    /**
     * The attribute-only pass, run the moment a report is filed.
     *
     * Deliberately carries no face distances: at this point the photograph has not reached
     * the server yet, and on a dead connection it never will. Their absence is "cannot
     * compare", which the engine scores as no signal — never as a mismatch — so a volunteer
     * with no signal still gets candidates ranked on the other nine attributes.
     */
    override suspend fun findCandidates(reportClientId: String): Outcome<List<LostFoundMatch>> =
        rankAndStore(reportClientId, faceDistances = emptyMap())

    /**
     * Ranks one report against the active opposite side and records the candidates.
     *
     * [faceDistances] is keyed by the opposite report's client id and arrives from
     * `process-face` immediately after enrolment. It is the same pass either way — running
     * a second, face-aware implementation alongside the attribute-only one is how the two
     * drift apart and start proposing different pairs for the same board.
     */
    private suspend fun rankAndStore(
        reportClientId: String,
        faceDistances: Map<String, Double>,
    ): Outcome<List<LostFoundMatch>> =
        withContext(dispatchers.io) {
            val subject = lostFoundDao.getByClientId(reportClientId)?.toDomain()
                ?: return@withContext Outcome.Failure(AppError.NotFound())

            val pool = lostFoundDao.getActiveByKind(subject.kind.opposite.wireName)
                .map { it.toDomain() }

            // On-device distances underneath, server distances on top. Offline or with the
            // CV service unconfigured the server map is empty and the local one carries the
            // whole face signal; when both exist the server's Facenet distance wins for the
            // candidates it covered and the local pass fills in the rest.
            val faces = localFaceDistances(subject, pool) + faceDistances

            val ranked = matchingEngine.rank(subject, pool, faceDistances = faces)

            val now = clock.nowEpochMillis()
            val created = mutableListOf<LostFoundMatchEntity>()

            ranked.forEach { candidate ->
                val lostId = if (subject.kind == LostFoundKind.LOST) {
                    subject.clientId
                } else {
                    candidate.report.clientId
                }
                val foundId = if (subject.kind == LostFoundKind.LOST) {
                    candidate.report.clientId
                } else {
                    subject.clientId
                }

                val existing = matchDao.findPair(lostId, foundId)

                // A pair a human has already ruled on is never re-raised. Without this the
                // engine would re-notify both volunteers on every pass, and a rejected
                // candidate would keep coming back.
                if (existing != null && existing.status != MatchStatus.CANDIDATE.wireName) {
                    return@forEach
                }

                val entity = LostFoundMatchEntity(
                    clientId = existing?.clientId ?: UUID.randomUUID().toString(),
                    serverId = existing?.serverId,
                    lostReportClientId = lostId,
                    foundReportClientId = foundId,
                    overallScore = candidate.score.overall,
                    confidence = candidate.score.confidence.name,
                    signalsJson = candidate.score.signals.toJson(),
                    status = MatchStatus.CANDIDATE.wireName,
                    createdAtEpochMillis = existing?.createdAtEpochMillis ?: now,
                    syncState = SyncState.PENDING.name,
                )

                matchDao.upsert(entity)
                created += entity
            }

            if (created.isNotEmpty()) {
                // The report is now under review rather than merely open, on both sides.
                lostFoundDao.setStatus(reportClientId, LostFoundStatus.MATCHED.wireName)
                syncScheduler.requestSync()
            }

            Outcome.Success(created.map { it.toDomain() })
        }

    /**
     * Compares this report's photograph against every photograph on the opposite side,
     * entirely on this device.
     *
     * Only reports whose photograph is on this phone can take part, which in practice is
     * the case that matters: a volunteer files the Found report for the child standing next
     * to them, and the Lost report was filed on the same handset at the help point. Anything
     * without a local descriptor simply contributes no face signal - which the engine scores
     * as "cannot compare", never as a mismatch, so those candidates still rank on the other
     * nine attributes.
     *
     * Never throws. A descriptor that cannot be computed is an absent signal, not an error,
     * and it must not take the whole ranking pass down with it.
     */
    private fun localFaceDistances(
        subject: LostFoundReport,
        pool: List<LostFoundReport>,
    ): Map<String, Double> {
        if (subject.subjectType != LostFoundSubjectType.PERSON) return emptyMap()

        val mine = runCatching {
            (FaceSignature.of(subject.photoLocalPath) as? FaceSignature.Result.Ready)?.descriptor
        }.getOrNull() ?: return emptyMap()

        return pool.mapNotNull { candidate ->
            if (candidate.clientId == subject.clientId) return@mapNotNull null
            if (candidate.subjectType != LostFoundSubjectType.PERSON) return@mapNotNull null

            val theirs = runCatching {
                (FaceSignature.of(candidate.photoLocalPath) as? FaceSignature.Result.Ready)
                    ?.descriptor
            }.getOrNull() ?: return@mapNotNull null

            candidate.clientId to FaceSignature.distance(mine, theirs)
        }.toMap()
    }

    override suspend fun reviewMatch(
        matchClientId: String,
        verdict: MatchStatus,
        note: String?,
    ): Outcome<LostFoundMatch> = withContext(dispatchers.io) {
        if (verdict == MatchStatus.CANDIDATE) {
            return@withContext Outcome.Failure(
                AppError.Validation(
                    field = "verdict",
                    message = "Confirm or reject the match.",
                ),
            )
        }

        val existing = matchDao.getByClientId(matchClientId)
            ?: return@withContext Outcome.Failure(AppError.NotFound())

        val reviewerId = supabase.auth.currentUserOrNull()?.id
            ?: return@withContext Outcome.Failure(AppError.Unauthorised())

        val now = clock.nowEpochMillis()
        val reviewed = existing.copy(
            status = verdict.wireName,
            reviewedBy = reviewerId,
            reviewedAtEpochMillis = now,
            reviewNote = note?.trim()?.ifBlank { null },
            syncState = SyncState.PENDING.name,
        )
        matchDao.upsert(reviewed)

        // Only a human confirmation closes a case. Facial similarity, however strong,
        // never gets here on its own.
        if (verdict == MatchStatus.CONFIRMED) {
            // Gamification: Award XP for confirming a match
            rewardRepository.awardXp(
                amount = com.varisahayak.domain.model.RewardEngine.XP_ASSIST_LOST_FOUND,
                reason = "Confirmed Lost & Found match",
                relatedEntityId = matchClientId
            )
            rewardRepository.recordImpact(peopleAssisted = 2) // Both sides helped

            lostFoundDao.setStatus(
                existing.lostReportClientId,
                LostFoundStatus.REUNITED.wireName,
            )
            lostFoundDao.setStatus(
                existing.foundReportClientId,
                LostFoundStatus.REUNITED.wireName,
            )
            lostFoundDao.setSyncState(existing.lostReportClientId, SyncState.PENDING.name)
            lostFoundDao.setSyncState(existing.foundReportClientId, SyncState.PENDING.name)
        } else {
            // A rejection returns both reports to the pool. Neither underlying report is
            // altered by having been wrongly paired.
            listOf(existing.lostReportClientId, existing.foundReportClientId).forEach { id ->
                // Counts every candidate, not just unsynced ones. A report whose other
                // candidate had already reached the server would otherwise be reopened
                // while a volunteer was still reviewing it.
                if (matchDao.countCandidatesFor(id) == 0) {
                    lostFoundDao.setStatus(id, LostFoundStatus.OPEN.wireName)
                    lostFoundDao.setSyncState(id, SyncState.PENDING.name)
                }
            }
        }

        syncScheduler.requestSync()
        Outcome.Success(reviewed.toDomain())
    }

    // --- sync ---------------------------------------------------------------------------------

    override suspend fun syncPending(): Outcome<Unit> = withContext(dispatchers.io) {
        var failed = false

        lostFoundDao.getPendingSync().forEach { entity ->
            try {
                val saved = supabase.from("lost_found_items")
                    .upsert(entity.toUploadDto()) {
                        // Upsert on client_id, never insert: a retried send updates the same
                        // row instead of filing a second report for the same missing child.
                        onConflict = "client_id"
                        select()
                    }
                    .decodeSingle<LostFoundReportDto>()

                saved.id?.let { lostFoundDao.markSynced(entity.clientId, it) }
            } catch (cancellation: CancellationException) {
                throw cancellation
            } catch (error: Exception) {
                failed = true
                lostFoundDao.setSyncState(entity.clientId, SyncState.FAILED.name)
            }
        }

        custodyDao.getPendingSync().forEach { record ->
            try {
                supabase.from("lost_found_custody")
                    .upsert(record.toDto()) { onConflict = "client_id" }
                custodyDao.markSynced(record.clientId)
            } catch (cancellation: CancellationException) {
                throw cancellation
            } catch (error: Exception) {
                failed = true
            }
        }

        matchDao.getPendingSync().forEach { match ->
            try {
                val saved = supabase.from("lost_found_matches")
                    .upsert(match.toDto()) {
                        onConflict = "client_id"
                        select()
                    }
                    .decodeSingle<LostFoundMatchDto>()

                saved.id?.let { matchDao.markSynced(match.clientId, it) }
            } catch (cancellation: CancellationException) {
                throw cancellation
            } catch (error: Exception) {
                failed = true
            }
        }

        // Nothing is ever discarded. A record that could not be sent stays PENDING or
        // FAILED, stays visible, and is retried.
        if (failed) {
            Outcome.Failure(AppError.Network())
        } else {
            Outcome.Success(Unit)
        }
    }

    override suspend fun refreshFromServer(): Outcome<Unit> = withContext(dispatchers.io) {
        try {
            val now = clock.nowEpochMillis()

            supabase.from("lost_found_items")
                .select()
                .decodeList<LostFoundReportDto>()
                .forEach { lostFoundDao.reconcileFromServer(it.toEntity(now)) }

            // Server-side matching runs after face processing, so candidates raised there
            // reach the device here rather than only ever being computed locally.
            val remoteMatches = supabase.from("lost_found_matches")
                .select()
                .decodeList<LostFoundMatchDto>()
                .map { it.toEntity(now) }
                .filter { remote ->
                    // Never overwrite a verdict this device has recorded but not yet sent.
                    matchDao.getByClientId(remote.clientId)?.syncState != SyncState.PENDING.name
                }

            matchDao.upsertAll(remoteMatches)
            Outcome.Success(Unit)
        } catch (cancellation: CancellationException) {
            throw cancellation
        } catch (error: Exception) {
            Outcome.Failure(AppError.Network(cause = error))
        }
    }

    private fun buildIncidentDescription(details: ReportDetails): String = buildString {
        append(details.title.trim())
        details.approximateAge?.let { append(", approx $it years old") }
        details.clothingDescription?.takeIf { it.isNotBlank() }?.let { append(". Wearing $it") }
        details.qrLocationName?.takeIf { it.isNotBlank() }?.let { append(". Last seen near $it") }
    }

    private companion object {
        const val TAG = "LostFoundRepository"

        /** Bounded so a backlog drains over several passes instead of one long one. */
        const val MAX_FACE_RETRIES_PER_PASS = 5
    }
}

/** In-memory application of the structured filters from §7.24. */
private fun LostFoundReport.matches(criteria: AttributeSearch): Boolean {
    criteria.kind?.let { if (kind != it) return false }
    criteria.subjectType?.let { if (subjectType != it) return false }
    criteria.status?.let { if (status != it) return false }

    // A range filter excludes a report only when the report actually states an age. An
    // unknown age is unknown, not out of range — excluding it would hide the very reports
    // that most need a human eye.
    if (criteria.minAge != null && approximateAge != null && approximateAge < criteria.minAge) {
        return false
    }
    if (criteria.maxAge != null && approximateAge != null && approximateAge > criteria.maxAge) {
        return false
    }

    criteria.gender?.takeIf { it.isNotBlank() }?.let { wanted ->
        if (gender != null && !gender.equals(wanted, ignoreCase = true)) return false
    }
    criteria.language?.takeIf { it.isNotBlank() }?.let { wanted ->
        if (language != null && !language.equals(wanted, ignoreCase = true)) return false
    }

    if (criteria.routeSequenceFrom != null && routeSequence != null &&
        routeSequence < criteria.routeSequenceFrom
    ) {
        return false
    }
    if (criteria.routeSequenceTo != null && routeSequence != null &&
        routeSequence > criteria.routeSequenceTo
    ) {
        return false
    }

    val occurred = occurredAtEpochMillis ?: reportedAtEpochMillis
    criteria.fromEpochMillis?.let { if (occurred < it) return false }
    criteria.toEpochMillis?.let { if (occurred > it) return false }

    if (criteria.onlyWithPhoto && !hasPhoto) return false

    criteria.text?.trim()?.takeIf { it.isNotEmpty() }?.let { text ->
        val haystack = listOfNotNull(
            title, description, personName, clothingDescription,
            physicalDescription, language, qrLocationName, additionalNotes,
        ).joinToString(" ").lowercase()

        if (!haystack.contains(text.lowercase())) return false
    }

    return true
}

/**
 * The `process-face` request body.
 *
 * A photograph, never an embedding. A client-supplied vector would be trivially forged into
 * a match, so the client's only input to face matching is the image itself.
 *
 * [action] asks for enrolment *and* a search in one call. The deployed face service has no
 * route that searches by a stored record — `/v1/face/match` takes an image — so splitting
 * the two would mean uploading the same photograph twice over a field connection to learn
 * one extra thing. One upload, both answers.
 */
@Serializable
private data class FaceProcessingRequest(
    val report_client_id: String,
    val image: String,
    val action: String = "enrol_and_match",
)

/**
 * The on-device verdict in the vocabulary the rest of the system already speaks.
 *
 * [FaceSignature.Result.Unreadable] becomes SERVICE_UNAVAILABLE rather than INVALID_IMAGE on
 * purpose. It means *this device* could not decode the file - an unusual codec, a bitmap too
 * large for the heap - and that is a retryable local condition, not proof the photograph is
 * bad. SERVICE_UNAVAILABLE is never persisted, so the report stays PENDING and the server
 * still gets its chance to read the image.
 */
private fun FaceSignature.Result.toFaceMatchStatus(): FaceMatchStatus = when (this) {
    is FaceSignature.Result.Ready -> FaceMatchStatus.READY
    FaceSignature.Result.NoFace -> FaceMatchStatus.NO_FACE
    FaceSignature.Result.MultipleFaces -> FaceMatchStatus.MULTIPLE_FACES
    FaceSignature.Result.Unreadable -> FaceMatchStatus.SERVICE_UNAVAILABLE
}
