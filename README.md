# Telegram Video Stremio Addon

A high-performance, self-hosted **Stremio Addon** with an interactive **Web Configuration UI** that streams video files directly from your private Telegram channels and groups into Stremio without buffering, complete with automatic **TheMovieDatabase (TMDB)** posters, backdrops, ratings, and IMDb matching.

---

## Key Highlights

- 🖥️ **Interactive Web Configuration UI (`/configure`)**: No need to hardcode tokens or chat IDs into text files. Enter your credentials in a clean Web UI, click **Fetch Chats**, and visually choose the channels you want with checkboxes (just like TGTV).
- ⚡ **Zero-Buffering Direct MTProto Streaming**: Uses native Telegram MTProto with HTTP 206 Partial Content (Range Requests). The video starts playing in under a second; scrubbing/seeking is instantaneous.
- 🖼️ **Full TMDB & IMDb Metadata Integration**: Automatically sanitizes raw release tags (`1080p`, `WEB-DL`, `x264`, codecs, release groups) from filenames, searches TMDB, and matches official IMDb IDs.
- 📺 **Seamless Stremio Integration**:
  - Appears under the **official movie / TV show page** in Stremio alongside other addons (Torrentio, Cinemeta, etc.).
  - Adds dedicated **Telegram Movies** and **Telegram Series** catalog rows on your Stremio home screen.
- 📡 **Real-time Live Sync**: Automatically captures, indexes, and surfaces newly posted or forwarded videos the moment they land in your Telegram channel.
- 🐳 **Docker-Ready**: Comes with Docker Compose, persisting your database and Telegram sessions safely in `./data`.

---

## Prerequisites (Get Free in 2 Minutes)

1. **Telegram App Credentials (`api_id` and `api_hash`)**:
   - Log in with your phone number at [my.telegram.org](https://my.telegram.org).
   - Click **API development tools** and create an app (e.g. `HomeStremio`).
   - Copy your numeric `api_id` and 32-character hex `api_hash`.

2. **Telegram Bot Token**:
   - Open Telegram and search for [@BotFather](https://t.me/BotFather).
   - Send `/newbot` and follow the prompts to choose a name and username.
   - Copy your **Bot Token** (e.g. `1234567890:ABCdefGHI...`).
   - **CRITICAL STEP — Disable Bot Privacy:**
     - In your chat with `@BotFather`, send `/setprivacy`.
     - Select your newly created bot.
     - Choose **Disable**. *(This allows the bot to receive video messages posted to channels)*.

3. **Telegram Channel**:
   - Create your private Telegram channel or group.
   - Add your bot to the channel and **promote it to Administrator** with "Read Messages" permission.
   - *(Optional)* Send a quick text or forward a video to your channel so the bot has an active event.

4. **TMDB API Key (Optional but Recommended)**:
   - Create a free account at [themoviedb.org](https://www.themoviedb.org/).
   - Go to **Settings → API** and generate a free Developer API key (v3 auth).

---

## Quick Start (Docker Compose — Recommended)

### 1. Configure the Network Environment
Navigate into the addon folder:
```bash
cd stremio-telegram-addon
cp .env.example .env
```

Open `.env` and verify your server's IP address:
```env
SERVER_HOST=0.0.0.0
SERVER_PORT=7000

# The LAN IP or domain of your home server (reachable by your TV/Phone/PC)
PUBLIC_URL=http://192.168.1.100:7000
```

### 2. Start the Server
```bash
docker compose up -d --build
```

### 3. Open the Configuration Web UI
Open your browser and visit:
```text
http://<YOUR-HOME-SERVER-IP>:7000/configure
```
*(or `http://localhost:7000/configure` if running on your local machine)*

---

## Using the Configuration Web UI

The Web UI provides a clean 3-step setup:

```
┌────────────────────────────────────────────────────────┐
│               Telegram Library TV Setup               │
│                                                        │
│  1. Telegram Credentials                               │
│     API ID:     [ 12345678                           ] │
│     API Hash:   [ a1b2c3d4e5f6g7h8i9j0...            ] │
│     Bot Token:  [ 1234567890:ABCdefGHI...            ] │
│                                                        │
│     [ 🔄 Connect & Fetch Available Chats ]             │
│                                                        │
│  ┌─ Choose Chats ────────────────────────────────────┐ │
│  │  [ Search chats...                              ] │ │
│  │                                                   │ │
│  │  ☑ STREAM HUB                   [Channel]         │ │
│  │    ID: -1001987654321                             │ │
│  │  ☐ BOSS                         [Chat]            │ │
│  │    ID: 987654321                                  │ │
│  │                                                   │ │
│  │  1 selected                  [🔄 Refresh List]     │ │
│  └───────────────────────────────────────────────────┘ │
│                                                        │
│  2. TMDB API Key: [ your_tmdb_api_key ] [🔍 Verify]   │ │
│     ✓ TMDB API Key verified successfully!             │
│                                                        │
│     [ 💾 Save Configuration & Enable Addon ]           │
│                                                        │
│  🎉 🚀 [ Install to Stremio (1-Click) ]                │
└────────────────────────────────────────────────────────┘
```

1. **Enter API ID, API Hash, and Bot Token**:
   Paste the credentials you obtained from `my.telegram.org` and `@BotFather`.
2. **Click "Connect & Fetch Available Chats"**:
   The addon contacts Telegram and automatically lists all channels, groups, and direct chats the bot is a part of.
3. **Select Your Channels**:
   - Check the box next to your movie/TV channels.
   - **If your private channel is not listed yet:** Simply enter its Channel ID (e.g. `-1001987654321`) or `@username` in the *Add Channel by ID* field and click **➕ Add Channel** — the addon will immediately verify bot admin access, fetch its real title, and select it! (You can also forward any video from that channel to the bot in Telegram and click *Refresh List*).
4. **Enter & Verify TMDB API Key**:
   Paste your TMDB key and click **🔍 Verify Key**. Only valid, verified keys are accepted to guarantee that your posters and metadata load properly in Stremio.
5. **Click "Save Configuration & Enable Addon"**:
   The server connects to Telegram via MTProto, begins indexing in the background, and gives you:
   - **"Install to Stremio (1-Click)"** button (opens the Stremio app and installs immediately).
   - **"Copy Manifest Link"** button (for manually pasting into Stremio).

---

## Installing into Stremio

### Method A: One-Click Install
From the device where you have Stremio installed, open the Web UI at `http://<server-ip>:7000/configure` and click **🚀 Install to Stremio**.

### Method B: Manual URL Paste
1. Open Stremio (on Android TV, Fire TV, PC, Mac, Linux, or Mobile).
2. Go to **Addons** (the puzzle piece icon).
3. In the search box / Addon URL bar, paste your manifest link:
   ```text
   http://<YOUR-HOME-SERVER-IP>:7000/manifest.json
   ```
4. Click **Install**.

---

## How It Appears in Stremio

1. **Under Official Movie & TV Pages:**
   When you search for or open *Inception* or *Breaking Bad* in Stremio, our addon checks your library. If the file is present in your channel, your private stream appears right under the stream choices:
   ```text
   [Telegram] Inception.2010.1080p.mkv
   💾 2.45 GB
   ```

2. **Under Home Screen Catalogs:**
   In your Stremio Discover tab, you will find:
   - **Telegram Movies**
   - **Telegram Series**
   Displaying official high-resolution posters, ratings, synopsis, and backdrops.

3. **TV Show Episodes:**
   Files with season and episode tags (e.g. `S01E02`, `1x05`) are automatically placed under their respective season/episode in Stremio's series player.

---

## Why Older Channel Files Aren't Seen by Bots (& How to Fix It)

In Telegram's MTProto architecture, a **Bot** account has a strict server-enforced privacy rule:
> **Bots CANNOT read channel history created before the bot was added to the channel.**

The original Telegram Video TV (TGTV) developer noted this exact constraint in `TdLib.kt`:
> *"Bots cannot read history in TDLib 1.8+ (GetChatHistory / SearchChatMessages are user-only), so a bot session sees only messages posted while it is connected. For the full history use 'Pair via phone'..."*

### Solution 1: The 10-Second Telegram Forward Workaround (Recommended)
You do not need to re-upload any files:
1. Open your channel on Telegram (Desktop or Mobile).
2. Right-click or long-press your older video posts and click **Forward**.
3. Send them right back into the channel (or to your Bot's private DM).
4. Telegram assigns them brand new message IDs with the current timestamp. The addon will immediately detect, match on TMDB, and add them to your Stremio library!

### Solution 2: User Account Mode (Full Channel History Mode)
If you have hundreds of older videos and want the addon to scan all messages back to the beginning of the channel:
1. In your terminal, run:
   ```bash
   docker compose exec stremio-addon python login_user.py
   ```
2. Enter your phone number and the login code sent to your Telegram app.
3. This creates a user session string (just like TGTV's "Pair via phone" mode).
4. Restart the container:
   ```bash
   docker compose restart
   ```
The addon will now scan 100% of your channel history from day one.

---

## Ultra-Fast Streaming Architecture (Zero Buffering)

```
┌─────────────────┐       HTTP 206 (Range: bytes=X-Y)       ┌────────────────────────────────┐
│  Stremio Player │ ──────────────────────────────────────► │  FastAPI Addon Server          │
│ (ExoPlayer/MPV) │ ◄────────────────────────────────────── │  • 128 MB RAM BlockCache (0ms) │
└─────────────────┘             Piped Byte Chunks           │  • 3-Chunk Prefetch Pipeline   │
                                                            └───────────────┬────────────────┘
                                                                            │ MTProto
                                                                            ▼ (512 KB chunks)
                                                                ┌────────────────────────┐
                                                                │ Telegram Media Servers │
                                                                └────────────────────────┘
```

1. **Instant RAM Start (`BlockCache`)**:
   Video players probe the first 64 KB (header) and last 512 KB (index atom), then re-request byte 0 to start playback. We cache blocks in RAM so the initial 1-2 MB is delivered in **0.001 ms** directly from memory, eliminating startup buffering completely.

2. **Asynchronous Prefetch Queue (`asyncio.Queue`)**:
   While the player streams Chunk 1, a background worker is already downloading Chunks 2 and 3 from Telegram. Network idle latency between chunks is eliminated, saturating playback bandwidth.

3. **Hardware Acceleration (`tgcrypto`)**:
   Raw Telegram packets are decrypted in C using AES-CTR at speeds exceeding 80 MB/s.

---

## Troubleshooting & FAQ

| Question / Issue | Solution |
| :--- | :--- |
| **"Fetch Chats" returns an empty list** | Bots only learn about chats that have messaged them or where they've been added. Send a message to the bot or forward a video to your channel, then click **Fetch Available Chats** again. You can also use the **+ Add ID manually** button. |
| **Why only videos sent after bot setup appear?** | Telegram restricts bots from viewing messages sent before they joined the channel. Simply forward your older videos in Telegram, or use User Mode (`python login_user.py`). |
| **"Failed to fetch" in Stremio** | Ensure `PUBLIC_URL` in `.env` is set to your server's actual local IP address (e.g. `http://192.168.1.100:7000`) instead of `127.0.0.1` or `localhost`. |
| **Are files stored on my home server?** | No. Videos are streamed directly from Telegram's servers through memory. Only lightweight metadata and thumbnails are stored in `./data/library.db`. |
| **Telegram file size limit** | Telegram allows files up to **2 GB** for standard accounts and bots (or 4 GB with Telegram Premium). |
