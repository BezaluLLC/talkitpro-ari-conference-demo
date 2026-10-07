# PBXware ARI Conference Demo

A web page and small Node.js server that run a conference call on PBXware through the
Asterisk REST Interface (ARI). The conference stays up while participants are dialed in
and replaced.

## What it does

1. **Start conference** creates a mixing bridge and calls an internal extension. When the
   extension answers, it joins the bridge.
2. The server then calls the external number. The extension hears a ring tone until the
   external party answers and joins.
3. Entering a new number and pressing **Dial** hangs up the current external party and calls
   the new number into the same bridge. The extension stays connected throughout.
4. **End conference** hangs up all participants and removes the bridge.

The page shows each participant's status, a list of finished calls with their hangup cause,
and a live log of ARI events.

## How it works

The server registers a Stasis application over the ARI WebSocket and holds the ARI
credentials. The page only talks to the server: it sends commands to `/api/*` and receives
state updates as Server-Sent Events. The ARI credentials never reach the browser, and
Asterisk needs no CORS configuration.

Each participant is a channel originated into the Stasis application. When a channel
answers, it enters the application and the server adds it to the bridge. The server creates
the bridge itself, so the bridge stays up while participants come and go.

## Requirements

- Node.js 22 or later. The server uses the built-in `fetch` and `WebSocket` and has no npm
  dependencies.
- ARI enabled on the PBXware server, with an ARI user.
- Network access from the machine running the demo to the PBXware HTTP server (port 8088
  by default).

## Setup

```bash
cp config.example.json config.json
# Edit config.json: ARI address, credentials, tenant, caller ID
node server.js
```

Open http://localhost:3000.

## Configuration

| Key | Description |
|---|---|
| `port` | Port for the demo page. Default `3000`. |
| `ari.url` | Address of the Asterisk HTTP server, for example `http://pbx.example.com:8088` or `https://pbx.example.com`. A trailing `/ari` is optional. |
| `ari.username`, `ari.password` | ARI user credentials. |
| `ari.app` | Stasis application name. Also used as the prefix for channel and bridge IDs. |
| `tenant` | PBXware tenant code, substituted for `{tenant}` in the endpoint templates. Leave empty on single-tenant systems. |
| `endpoints.internal` | Dial string for extensions. `{tenant}` and `{number}` are substituted. |
| `endpoints.external` | Dial string for external numbers. Same substitutions. |
| `callerId` | Caller ID sent on outgoing calls. Use a number the tenant owns, or carriers may reject the call. |
| `dialTimeout` | Seconds to ring before giving up. |
| `ringbackMedia` | Sound played into the bridge while the external party rings. Set to `""` to turn it off. |

`config.json` is excluded from Git because it holds credentials.

## Dialing on PBXware

On multi-tenant PBXware, each SIP device is named after its tenant code and extension:
extension 104 on tenant 100 is `PJSIP/100104`. Calls from a tenant's extensions run in the
dialplan context `t-<tenant code>`.

Send both participants through that context:

```json
"tenant": "100",
"endpoints": {
  "internal": "Local/{number}@t-{tenant}",
  "external": "Local/{number}@t-{tenant}"
}
```

Calls then follow the same rules as a call placed from one of the tenant's phones. External
numbers use the tenant's outbound routes, and extensions ring the way they are configured to,
including mobile apps woken by push notification.

Avoid dialing devices directly with `PJSIP/{tenant}{number}`. That bypasses PBXware's pre-dial
setup. In testing, Bicom's Communicator Next rang but could not answer, and mobile apps were
not rung.

Because calls go through the dialplan, the extension's forwarding and voicemail settings
apply. If an extension doesn't answer, its voicemail can pick up and join the conference.
Set `dialTimeout` shorter than the extension's ring time before voicemail.

To find tenant codes and contexts on a different system, run `pjsip show endpoints` and
`dialplan show` from the Asterisk CLI.

## Input handling

Numbers entered on the page may contain only digits, `+`, `*` and `#`. Spaces, dashes,
dots and parentheses are removed. This stops a number from adding a different context
(`@...`) or a second channel (`&...`) to the dial string. The tenant code is validated at
startup.

## Limitations

- One conference at a time.
- State is held in memory. Restarting the server loses track of the conference; stopping it
  with Ctrl+C hangs up all participants first.
- If the ARI WebSocket disconnects, the server reconnects every 3 seconds. Calls in progress
  continue, but events during the outage are lost.
- Calls entering the Stasis application from elsewhere are logged and otherwise ignored.
