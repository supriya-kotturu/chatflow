# Prompts Used in Chat-Flow Project

## Development Prompts

### JavaScript/Frontend
1. **Path Parameter Analysis**: "what happens when htven the getRoomId from path returns the last segment after splitting the path by &quot;/"
2. **URL Structure Testing**: "http://localhost:3000/chat/sdsfh/ssssssss"
3. **404 Debugging**: "but the browser shows 404, why?"
4. **Routing Strategy**: "which approach is better? query params or path?"
5. **Route Confirmation**: "I'll stick with chat/id"
6. **Route Restriction**: "no, I dont want nested routes to chat/34"

### Documentation
7. **Prompt Tracking**: "add the list of prompts used in docs/PROMPTS.md. Keep track of all the promps"

## Technical Decisions Made

- **URL Structure**: Chose path parameters (`/chat/roomId`) over query parameters for cleaner URLs
- **Route Pattern**: Decided on exact match `/chat/:roomId` to prevent nested routes
- **WebSocket Integration**: Room ID extracted from URL path for WebSocket connection

## Code Snippets Generated

### Server Route Configuration
```javascript
app.get('/chat/:roomId', (req, res) => {
    res.sendFile(path.join(__dirname, 'html/index.html'));
});
```

### Alternative Query Parameter Approach (Not Used)
```javascript
function getRoomIdFromPath() {
    return new URLSearchParams(window.location.search).get('room');
}
```

---
*Last updated: [Current Date]*
*Total prompts tracked: 7*