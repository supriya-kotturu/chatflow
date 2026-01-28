# NOTES

- Trying to access the websocket URI, through browser doesn't work
  - The browser ALWAYS makes a `http/s` request and doesn't "upgrade" the http connection
  - Using websocket client through a script(JS) would allow us to trigger the target ws endpoint
  - We can't have same URI for both ws connection and http connection
  - We can instead -
    - create a html page with a `/chat/${roomId}` to serve a http response
    - add a script in the html page to start a ws connection to `/ws/chat/${roomId}`
    - both the URI have different handlers where
      - `/chat/${roomId}` serves a html page
      - `/ws/chat/${roomId}` listens to the ws connection