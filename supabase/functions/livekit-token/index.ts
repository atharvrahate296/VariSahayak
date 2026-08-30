import { withSupabase } from 'npm:@supabase/server@^1'

/**
 * Mints a short-lived LiveKit join token for the walkie-talkie.
 *
 * This function exists for exactly one reason: the LiveKit API secret must never reach a
 * phone. A signing key compiled into an APK is a public key — anyone can unzip the app,
 * read it, and mint themselves a token for the emergency channel from a laptop. Keeping the
 * secret here means the only way onto a channel is through a Supabase session this project
 * already trusts.
 *
 * `auth: 'user'` with `verify_jwt = true`: an unauthenticated call is rejected by the
 * platform before any code runs and before LIVEKIT_API_SECRET is ever read.
 *
 * Beyond being signed in there is no gate. Any signed-in user may join any channel,
 * including Emergency, and that is deliberate — in an emergency the person who needs to
 * speak is whoever is standing there, and a role check would be a way for the product to
 * refuse the one transmission that mattered. What the token does NOT grant is the ability
 * to administer a room, mute anyone, or record: `roomAdmin` and `roomRecord` are absent.
 */

/**
 * The rooms that exist, and the only values this will sign for.
 *
 * An allowlist rather than passing the caller's string through. Without it, a client could
 * mint tokens for arbitrary room names and use the server as free conferencing — and
 * two clients that disagreed about a channel id would silently land in different rooms and
 * hear nothing, which is exactly the failure a radio must never have.
 *
 * Must stay in step with WalkieChannels.ALL in the Android app.
 *
 * 'route-main' is the old id of what is now 'comm-1'. It is kept signable so that a phone
 * still running a pre-rename APK is not dropped off the radio the moment this function is
 * redeployed — a stale build losing its channel is a silent failure in the field, and the
 * cost of one extra string here is nothing. Delete it once every device has been updated.
 */
const CHANNELS = ['comm-1', 'medical', 'emergency', 'route-main'] as const

/**
 * Ten minutes.
 *
 * Only needs to outlive the connection handshake — LiveKit checks the token at join and
 * keeps the session alive afterwards, so a short expiry costs a reconnecting client one
 * extra function call and costs a leaked token almost nothing.
 */
const TOKEN_TTL_SECONDS = 600

const bad = (message: string, status = 400) =>
  Response.json({ ok: false, message }, { status })

const base64Url = (bytes: Uint8Array): string => {
  let binary = ''
  for (const byte of bytes) binary += String.fromCharCode(byte)
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
}

const encodeSegment = (value: unknown): string =>
  base64Url(new TextEncoder().encode(JSON.stringify(value)))

/**
 * Signs a LiveKit access token.
 *
 * Hand-rolled over Web Crypto rather than pulling in a JWT library: this is one HS256
 * signature over two known segments, and a dependency here would be a third-party package
 * with access to the signing secret.
 *
 * The claim shape is LiveKit's: `iss` is the API key, `sub` is the participant identity,
 * and the `video` grant carries the room permissions.
 */
async function signAccessToken(
  apiKey: string,
  apiSecret: string,
  identity: string,
  name: string,
  room: string,
): Promise<string> {
  const now = Math.floor(Date.now() / 1000)

  const header = { alg: 'HS256', typ: 'JWT' }
  const payload = {
    iss: apiKey,
    sub: identity,
    // LiveKit reads `name` for the participant's display name, which is what the Android
    // widget renders in "Amit + 2 others". Without it the label would show a raw UUID.
    name,
    nbf: now - 10, // Tolerates a little clock skew between Supabase and the VM.
    exp: now + TOKEN_TTL_SECONDS,
    video: {
      room,
      roomJoin: true,
      canPublish: true,
      canSubscribe: true,
      // Data messages are unused today. Left on because it costs nothing and turning it
      // off later is easier than diagnosing why a future ping does not arrive.
      canPublishData: true,
      // Deliberately absent: roomAdmin, roomCreate, roomList, roomRecord, canUpdateOwnMetadata.
      // A participant token should not be able to reshape the room it is joining.
    },
  }

  const signingInput = `${encodeSegment(header)}.${encodeSegment(payload)}`

  const key = await crypto.subtle.importKey(
    'raw',
    new TextEncoder().encode(apiSecret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign'],
  )

  const signature = await crypto.subtle.sign(
    'HMAC',
    key,
    new TextEncoder().encode(signingInput),
  )

  return `${signingInput}.${base64Url(new Uint8Array(signature))}`
}

export default {
  fetch: withSupabase({ auth: 'user' }, async (req: Request, ctx: any) => {
    if (req.method !== 'POST') return bad('Unsupported request.', 405)

    const apiKey = Deno.env.get('LIVEKIT_API_KEY')
    const apiSecret = Deno.env.get('LIVEKIT_API_SECRET')
    const url = Deno.env.get('LIVEKIT_URL') ?? ''

    if (!apiKey || !apiSecret) {
      // Configuration, not a caller error. Logged without the values.
      console.warn('LIVEKIT_API_KEY / LIVEKIT_API_SECRET are not set; the radio is disabled.')
      return Response.json(
        { ok: false, message: 'The radio is not configured.' },
        { status: 503 },
      )
    }

    let payload: { room?: string }
    try {
      payload = await req.json()
    } catch {
      return bad('Could not read the request.')
    }

    const room = (payload.room ?? '').trim()
    if (!CHANNELS.includes(room as (typeof CHANNELS)[number])) {
      return bad('Unknown channel.')
    }

    // The identity comes from the verified JWT, never from the body. If a client could
    // name itself, every active-speaker label on every device would be worth nothing.
    //
    // Two sources, in order, because `ctx.userClaims` is not populated by every
    // @supabase/server 1.x build — and when it is absent this function used to reject a
    // perfectly valid signed-in caller with "Not signed in.", which is a confusing thing
    // to debug from a phone. `auth.getUser()` reads the same Authorization header the
    // platform already verified, so the fallback is no weaker than the primary: both
    // derive the id from the JWT, neither trusts the request body.
    const identity: string | undefined =
      ctx.userClaims?.sub ??
      (await ctx.supabase.auth.getUser()).data?.user?.id

    if (!identity) return bad('Not signed in.', 401)

    // The display name is what other volunteers see when this person keys the mic, so it
    // is read from the profile rather than taken on trust. ctx.supabase is RLS-scoped to
    // the caller, so this can only ever read their own row.
    const { data: profile } = await ctx.supabase
      .from('profiles')
      .select('display_name')
      .eq('id', identity)
      .maybeSingle()

    // Falls back to a shortened id rather than to something friendly-but-wrong. A radio
    // label that names the wrong person is worse than one that names nobody.
    const name = (profile?.display_name ?? '').trim() || `Volunteer ${identity.slice(0, 8)}`

    const token = await signAccessToken(apiKey, apiSecret, identity, name, room)

    return Response.json({
      ok: true,
      token,
      // Returned for parity and debugging only. The Android client connects to the address
      // compiled into its BuildConfig, because the debug build's cleartext exception is
      // pinned to that one host and following a server-supplied address would either fail
      // opaquely or widen what the app will talk to in plaintext.
      url,
      identity,
      name,
      expires_in: TOKEN_TTL_SECONDS,
    })
  }),
}
