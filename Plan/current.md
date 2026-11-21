# Stabilize Floppy's existing chat

Fix session ownership and event ordering first. The server can process an event after its user ID has been reassigned. This already affects presence and message attribution. It also matters for the private delivery planned in [the feature roadmap](future.md).

## Review baseline

Reviewed both binaries, their modules, the workspace manifests, lockfile, ignore rules, and the relevant framing behavior of the locked `flpc` revision at Floppy commit `218a8c3`.

| Check | Observed result |
| --- | --- |
| `cargo test --locked --workspace --all-targets --offline` | Both crates compiled, with warnings. Both ran zero tests. |
| `cargo fmt --all -- --check` | Failed. Both crates have formatting differences. |
| `cargo clippy --locked --workspace --all-targets --offline -- -D warnings` | Failed with 29 server and 9 client diagnostics, counting each crate's findings once. |
| Panic-site search | Two explicit `unreachable!()` calls in workspace source, both in server login. |

The concurrency findings below come from tracing the code. The proposed regression tests have yet to run. Keep the current workspace split, framed transport, bounded channels, bounded client scrollback, and the outbound event-to-frame conversion. There is enough structure to repair the lifecycle without rewriting the application.

## 1. Give the server one owner for sessions and routing

| Priority | Finding and consequence | Repair |
| --- | --- | --- |
| High | A connection enqueues `Connected(id)` and later removes its registry entry independently of the router. A delayed join lookup can find no entry or a different username. A chat event also identifies its sender by reusable ID alone. | Move live-session allocation, admission, routing and removal into the existing event loop. Give each connection a fresh internal session token. Validate that token before acting on a command and order the old user's leave before releasing its wire ID. |
| High | Installing a writer, taking a roster snapshot, announcing presence and marking a user onboarded are separate operations. Presence and public chat select different sets of recipients during that interval. A joining user can see duplicate presence or miss a public message. | In one router operation, admit the user, capture the roster including self, and subscribe the connection to subsequent events. Give the writer the snapshot as one batch. Send the snapshot and join success before draining live events. Announce the join once to existing peers. |
| High | The single router awaits each writer queue in turn. A full queue can prevent every other user from receiving messages, joins or leaves. | Use bounded nonblocking enqueue. Collect full or closed sessions and evict them through normal cleanup after fan-out. Actively cancel their socket tasks. Preserve FIFO delivery to healthy peers. Send the initial roster separately so the 256-user default fits alongside a queue capacity of 128. |
| Medium | A connection waiting for its first frame consumes a task before the user-capacity check. A failed listener accept silently ends the accept loop. The reader and writer swallow transport errors, so many failures are logged as normal closure. | Limit pending handshakes separately from active sessions. Apply a deadline through the initial roster write. Supervise the listener, back off on transient failure, and surface terminal failure. Distinguish EOF, malformed frames, I/O failure, timeout and slow-consumer eviction. |

The current routing code already releases its registry lock before awaiting sends. The stall comes from the router waiting on a recipient's queue. [Connection lifecycle](../server/src/main.rs#L54) · [Recipient selection and fan-out](../server/src/message/route.rs#L9)

Extract `serve`, `handle_connection`, and connection read/write loops. Let a synchronous `SessionRegistry` live inside the router task. Connection tasks own sockets and submit commands. Centralize teardown for handshake failures, read/write failures, cancellation and ordinary departure. If an admitted connection disappears before it receives the roster reply, reclaim its session and send a single leave to peers who saw the join. [Current registry](../server/src/users.rs#L56) · [Event types](../server/src/message/event.rs)

## 2. Repair client login, disconnect and terminal handling

| Priority | Finding and consequence | Repair |
| --- | --- | --- |
| High | Dropping the login keyboard task's handle detaches it. Chat immediately creates another event stream, so the old task can consume the next key. Resize and input EOF also have no explicit UI outcome. | Own one input stream across login and chat. Handle resize, Escape, input EOF and input error explicitly. Retain and join any spawned task. |
| High | On server EOF or I/O error the reader exits silently, while other senders keep the UI receiver alive. Login awaits the network inside the Enter handler. A stalled chat write also blocks UI processing. | Deliver an explicit disconnect reason to application state. Keep network waits cancellable during login and use bounded outbound work during chat. Treat malformed or unexpected frames according to the current protocol phase. |
| High | Raw mode and the alternate screen are set up before multiple fallible calls. A draw or initialization error can bypass cleanup. | Put partial terminal setup and restoration in one guard spanning both screens. Show the cursor, leave the alternate screen and disable raw mode on all exit paths, including unwinding. Attempt each applicable restoration step even if another fails. |
| Medium | Failed login usually exits before the user sees a reason. The login renderer does not display failure status. A single initial `UserLeft` clears the whole roster. | Make `run_login` return the connected transport and roster together, or an explicit cancellation. Show recoverable errors and allow retry with the same username. Stage a fresh roster per attempt and remove only the ID in a leave frame. |
| Medium | The client counts Unicode scalar values. The wire format counts UTF-8 bytes. The username UI allows 16 characters and the server allows 64 bytes. The chat UI's 256-character limit applies only to this client. Empty names and messages can pass. | Enforce documented application byte limits on the server and mirror them in the client. Reject blank names, blank messages and control characters in the single-line interface. Keep display-column calculations separate. Validate before clearing an editor and retain the draft on submission failure. |
| Lower | Approximate wrap height can hide recent lines or misplace the cursor. Narrow viewports can overflow the scroll conversion or leave no valid cursor position. A zero message limit still retains one line. | Base scroll and cursor positions on the rendering widget's wrapping rules, bound conversions, hide the cursor when the inner area is empty, redraw on resize, and let zero scrollback disable retention. |

Extract `run_login` and `run_chat` from client startup. Keep the window types responsible for rendering. Share the duplicated length-limited editor as `TextInput`. Separate keyboard actions from network events so login can handle its events exhaustively. Return a loop outcome instead of maintaining a second quit flag. [Client lifecycle](../client/src/main.rs#L39) · [Input stream](../client/src/input.rs#L21) · [Login form](../client/src/login.rs) · [Chat rendering and buffering](../client/src/chatwindow.rs#L68) · [Tokio task handle semantics](https://docs.rs/tokio/latest/tokio/task/struct.JoinHandle.html)

## 3. Simplify Rust types, errors and conventions

1. **Delete both wildcard arms in server login.** The match starting at line 39 already covers all three `CheckJoin` variants. The next match covers both `TryInsert` variants. Remove the arms at lines 43 and 53–55. A future enum variant should produce a compile-time exhaustiveness error. Keep real rejection paths for unexpected frames from the network. [Both match sites](../server/src/login/accept.rs#L39)
2. **Use the existing registry results.** Replace `TryInsert` and `TryRemove` with the `Result` values already produced by the registry methods. Preserve duplicate-name, capacity, malformed-input, timeout, EOF and I/O distinctions to the connection boundary. Map user-facing rejections to documented join-failure codes. Keep the underlying cause for diagnostics. [Registry wrappers](../server/src/users.rs#L153) · [Registry errors](../server/src/error.rs) · [Current failure responses](../server/src/login/accept.rs#L103)
3. **Simplify registry invariants.** Derive count from map length. Construct a live entry with its sender, expose roster and lookup operations, and keep the map private. At the default capacity, scanning for a free wire ID on admission is adequate. Remove the redundant cached count and free-ID bookkeeping. Move roster construction behind the registry/router boundary to eliminate the cross-dependency between login and users.
4. **Simplify delivery.** Replace the special last-recipient optimization and fallible reconstruction inside the fan-out loop with a shareable application event. Convert it into an owned wire frame in the writer. Keep the conversion near the existing `From<ServerEvent>` implementation. Preserve the current single local echo for the author.
5. **Make formatting a separate mechanical change.** Run rustfmt. Use names such as `MAX_USERS`, `LISTEN_ADDR`, `input_limit` and `message_limit`. Apply field shorthand, tail expressions, and direct boolean comparisons. Use `?` where the caller handles the error. Remove unused setters, commented imports, redundant explicit drops and unit bindings on `abort()`. Give intentional best-effort error handling a clear context.
6. **Review the manifests together.** Declare common dependency versions in workspace dependencies, enable the features each binary uses directly, and run checks for each package individually as well as the workspace. Keep the lockfile for reproducible application builds. [Workspace](../Cargo.toml) · [Client dependencies](../client/Cargo.toml) · [Server dependencies](../server/Cargo.toml)

## 4. Comment the contracts and test the repairs

| Where | What a comment should explain |
| --- | --- |
| Router admission and cleanup | When presence becomes visible, which events follow the snapshot, and when a wire ID may be reused. |
| Recipient queue handling | Why a full queue ends that session and why dropping individual presence events would corrupt the roster. |
| Validation and rendering | Which limits are UTF-8 bytes, which measurements are terminal cells, and where the server enforces each input rule. |
| Client lifecycle | Why the terminal and input stream span both screens, and which owner cancels network work on exit. |

Prefer names and small functions for ordinary control flow. Remove section comments such as “Retrieval” and obsolete TODOs once the behavior is implemented.

- **Router tests.** Drive overlapping admissions, an interrupted roster write, leave followed by immediate wire-ID reuse, delayed commands from an old session, and one full writer queue. Use controlled channels and barriers. Assert recipient identity, order and continued progress for healthy peers.
- **Protocol integration.** Use an ephemeral loopback listener to cover successful join, duplicate name, capacity, invalid first frame, handshake timeout, roster followed by live traffic, public broadcast, and abrupt disconnect.
- **Client tests.** Inject input and network events for retry, Escape during login, EOF without a keypress, one-user removal and transition to chat. Use Ratatui's test backend for tiny viewports, wide characters, combining marks, long words and resize. Smoke-test terminal restoration on an injected draw error.
- **Completion.** Run the three baseline commands again. Require clean formatting, warning-free strict Clippy, and passing regression tests in both crates before building private delivery on these semantics.
