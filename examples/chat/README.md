# raknet chat example

Small terminal chat built on top of `raknet`.

Start the server:

```sh
v run examples/chat/server -- 127.0.0.1:19132
```

Start clients in separate terminals:

```sh
v run examples/chat/client -- 127.0.0.1:19132 scher
v run examples/chat/client -- 127.0.0.1:19132 aisyx
```

This is intentionally simple. It exercises `listen`, `accept`, `dial`, `read_packet`,
`write`, connection close handling and basic broadcast flow.
