# ⚡ Teleflux

<div align="center">

![Version](https://img.shields.io/badge/version-2.0.0-blue?style=for-the-badge)
![Python](https://img.shields.io/badge/python-3.10%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.110%2B-009688?style=for-the-badge&logo=fastapi&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Stremio](https://img.shields.io/badge/Stremio-Addon_v2-7B52AB?style=for-the-badge)
![Nuvio](https://img.shields.io/badge/Nuvio-Compatible-FF4500?style=for-the-badge)
![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)

**The High-Performance, Zero-Buffering Telegram Media Streaming Engine for Stremio & Nuvio.**

*Stream 4K, 1080p, and multi-audio media directly from your private Telegram channels into any Stremio or Nuvio media center — featuring instant seek, real-time message indexing, token-encrypted access, and automated TMDB metadata matching.*

[Key Highlights](#-key-highlights) •
[Instant Real-Time Indexing](#-instant-real-time-indexing) •
[Self-Hosting](#-self-hosting-guide) •
[Installation](#-installation-into-stremio--nuvio) •
[Architecture](#-zero-buffering-streaming-architecture) •
[Adult Mode (Optional)](#-optional-adult-mode-teleflux-red) •
[Inspirations](#-inspirations--references) •
[Troubleshooting](#-troubleshooting--faq)

</div>

---

## 📖 Overview

**Teleflux** is an enterprise-grade, self-hosted media bridge that transforms your Telegram channels and groups into an ultra-fast streaming library. Rather than downloading heavy video files to local hard drives, Teleflux uses native Telegram MTProto byte streaming with in-memory RAM caching and HTTP 206 Partial Content (Range Requests).

Videos start playing in **under a second**, with instantaneous scrubbing and zero playback buffering across Android TV, Fire TV, Windows, macOS, Linux, iOS, and Web players.

Within Stremio and Nuvio, your media is cleanly surfaced under standard **Telegram Movies** and **Telegram Series** catalogs, while the addon engine itself runs under **Teleflux**.

---

## ✨ Key Highlights

- ⚡ **Zero-Buffering Playback**: Video playback initiates in **~300ms**. An in-memory 128 MB RAM BlockCache and an asynchronous 3-priority prefetch pipeline eliminate stuttering and network idle time.
- 🔄 **Real-Time Instant Channel Indexing**: Operates on live Telegram MTProto socket push events (`events.NewMessage`). New movies or episodes sent to your channel are detected and indexed in **real time** (typically 1–2 seconds).
- 📁 **Clean Standard Catalogs**: Media appears directly in your Stremio library under **Telegram Movies** and **Telegram Series**.
- 🔒 **Encrypted Security Token Protection**: Public URLs are secured with a 32-character cryptographic token (`/{security_token}/manifest.json`, `/{security_token}/play/...`). Anyone attempting to access your server's IP directly without the token is blocked with `403 Forbidden`. Includes 1-click token rotation.
- 🖥️ **Modern Dark Glassmorphism Dashboard**: Intuitive, responsive web interfaces (`/stremio` and `/settings`) featuring channel discovery, multi-channel selection, real-time indexing statistics, manual channel addition, and one-click installs.
- 🔍 **Automated TMDB Metadata Matching**:
  - Automatically cleans complex release filenames (`WEB-DL`, `HDRip`, `x265`, `10Bit`, `DDP5.1`, audio tags).
  - Matches official IMDb IDs, high-resolution posters, backdrops, plot summaries, and series season/episode numbers.
- 📺 **Universal Player Compatibility**: Runs smoothly on **Stremio** and **Nuvio** across:
  - 📺 Android TV / Google TV / Fire TV Stick
  - 💻 Windows / macOS / Linux / Steam Deck
  - 📱 Android Mobile / iOS (Web & External Players like Outplayer/VLC)
- 🚀 **Hardware-Accelerated Decryption**: Raw Telegram packets are decrypted in C using AES-CTR (`tgcrypto`) at speeds exceeding 80 MB/s.

---

## ⚡ Instant Real-Time Indexing

### How fast do new movies appear?
Teleflux does **not** rely on a slow polling cron job or periodic interval timer. Instead, Teleflux maintains an open, persistent MTProto socket connection with Telegram:

1. **Instant Push Notification**: The exact millisecond a video is posted or forwarded into your connected Telegram channel, Telegram pushes a `NewMessage` event over the socket to Teleflux.
2. **Instant Metadata Parsing & Matching**: Teleflux immediately extracts the filename and media specifications, queries TheMovieDatabase (TMDB) API for posters, backdrop, overview, and IMDb IDs, and saves the entry into the local SQLite database.
3. **Available in Stremio**: The entire process finishes in **1–2 seconds**. The next time you open or refresh your **Telegram Movies** or **Telegram Series** catalog in Stremio, the movie or episode is immediately ready to stream.

> [!NOTE]
> For historical videos sent before the bot joined the channel, Telegram's API prevents bots from reading past message history. Use the **🔄 Scan Channel History Now** button on the `/stremio` dashboard, or simply forward the older videos into the channel to trigger instant live indexing.

---

## 🛠️ Prerequisites

Setting up Teleflux requires free API credentials:

### 1. Telegram App Credentials (`api_id` & `api_hash`)
1. Log in with your phone number at [my.telegram.org](https://my.telegram.org).
2. Go to **API development tools** and create a new application (e.g. `TelefluxStream`).
3. Save your numeric `api_id` and 32-character `api_hash`.

### 2. Telegram Bot Token (`@BotFather`)
1. Open Telegram and message [@BotFather](https://t.me/BotFather).
2. Send `/newbot` and follow the instructions to name your bot.
3. Save your **Bot Token** (e.g. `9124514211:AAEGM30SQnNQ...`).
4. **CRITICAL STEP — Disable Bot Privacy:**
   - In your chat with `@BotFather`, send `/setprivacy`.
   - Select your bot.
   - Choose **Disable**. *(This grants the bot permission to detect video messages in channels)*.

### 3. Telegram Channels
1. Create or open your private Telegram channel.
2. Add your bot as an **Administrator** with *Read Messages* permission.
3. Post or forward a video into the channel so the bot has an active event.

### 4. Metadata API Key
- Get a free API Key (v3 auth) from [themoviedb.org](https://www.themoviedb.org/).

---

## 🚀 Self-Hosting Guide

### Method 1: Docker Compose (Recommended)

1. **Clone the repository:**
   ```bash
   git clone https://github.com/your-username/teleflux.git
   cd teleflux
   ```

2. **Configure environment:**
   ```bash
   cp .env.example .env
   ```
   Edit `.env` with your preferred editor:
   ```env
   SERVER_HOST=0.0.0.0
   SERVER_PORT=7001
   
   # LAN IP or public domain of your server (accessible by your TV/Phone/PC)
   PUBLIC_URL=http://192.168.1.16:7001
   DATA_DIR=./data
   
   # Telegram Credentials
   TELEGRAM_API_ID=12345678
   TELEGRAM_API_HASH=your_telegram_api_hash_here
   TELEGRAM_BOT_TOKEN=your_bot_token_here
   
   # Channels (comma-separated IDs, e.g. -1004474080975)
   CHANNEL_IDS=-1004474080975,-1003961034355
   
   # Metadata Key
   TMDB_API_KEY=your_tmdb_api_key_here
   ```

3. **Launch the container:**
   ```bash
   docker compose up -d --build
   ```

4. **Access the Web Dashboard:**
   Open your browser and navigate to:
   ```text
   http://<YOUR-SERVER-IP>:7001/stremio
   ```

---

### Method 2: Bare Metal / Python 3.10+

1. **Clone and navigate to repository:**
   ```bash
   git clone https://github.com/your-username/teleflux.git
   cd teleflux
   ```

2. **Create and activate a virtual environment:**
   ```bash
   # Linux / macOS
   python3 -m venv venv
   source venv/bin/activate

   # Windows (PowerShell)
   python -m venv venv
   .\venv\Scripts\Activate.ps1
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure your `.env`:**
   ```bash
   cp .env.example .env
   ```
   *(Fill in your credentials as shown in the Docker section above)*

5. **Start the server:**
   ```bash
   python main.py
   ```

---

## 🖥️ Web Management Dashboard

Access the management interface anytime at:
- **Streaming Dashboard:** `http://<SERVER-IP>:<PORT>/stremio`
- **Bot Credentials & Settings:** `http://<SERVER-IP>:<PORT>/settings`

```
┌────────────────────────────────────────────────────────────────────────┐
│  ⚡ Teleflux v2.0                          [ 📺 Stremio Addon ] [ ⚙️ ]  │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  [🔒 Token Protected Manifest]                                         │
│  🔗 http://192.168.1.16:7001/477ae794.../manifest.json                 │
│     [ 📋 Copy URL ]   [ 🎬 Install to Stremio ]   [ 🔄 Rotate Key ]    │
│                                                                        │
│  1. Select Streaming Channels                      [☑️ Select All] [🔄]│
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │ ☑️  📢 STREAM HUB          -1004474080975           [ CHANNEL ]  │  │
│  │ ☑️  📢 MOVIE ARCHIVE       -1003961034355           [ CHANNEL ]  │  │
│  │ ☐  💬 BACKUP GROUP        -1004421826021           [ GROUP ]    │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│  ➕ Add Channel Manually: [-1004474080975        ] [ Add Channel ]     │
│                                           [ 💾 Save Streaming Channels]│
│                                                                        │
│  2. Catalog Index & Sync                        ● System Ready         │
│  ┌───────────────┐  ┌───────────────┐  ┌───────────────┐               │
│  │      42       │  │      188      │  │      230      │               │
│  │ MOVIES INDEXED│  │EPISODES INDEX │  │TOTAL MEDIA    │               │
│  └───────────────┘  └───────────────┘  └───────────────┘               │
│  [ 🧹 Clear Catalog Index ]             [ 🔄 Scan Channel History Now ] │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 📱 Installation into Stremio & Nuvio

### Method A: One-Click Install
1. Open the Web Dashboard (`http://<SERVER-IP>:<PORT>/stremio`) on the device running Stremio or Nuvio.
2. Click **🎬 Install to Stremio**.
3. Your media player will launch and prompt you to install **Teleflux**.
4. Once installed, your library will display the **Telegram Movies** and **Telegram Series** catalogs.

### Method B: Manual Manifest URL
1. On the dashboard, click **📋 Copy** to copy your tokenized manifest link:
   ```text
   http://<YOUR-SERVER-IP>:<PORT>/<YOUR-TOKEN>/manifest.json
   ```
2. Open **Stremio** or **Nuvio**.
3. Navigate to the **Addons** tab (the puzzle icon).
4. Paste the copied URL into the search / URL bar and press **Enter** or **Install**.

---

## 🏎️ Zero-Buffering Streaming Architecture

Teleflux eliminates traditional downloading bottlenecks by piping encrypted Telegram MTProto chunks straight through an in-memory buffer directly into the video player:

```
┌────────────────────────┐
│ Stremio / Nuvio Player │
│ (ExoPlayer / MPV / VLC)│
└───────────┬────────────┘
            │ HTTP 206 Partial Content (Range: bytes=X-Y)
            ▼
┌────────────────────────────────────────────────────────┐
│ Teleflux Streaming Engine (FastAPI)                    │
│                                                        │
│  • 128 MB RAM BlockCache (0.001 ms seek response)      │
│  • Asynchronous 3-Priority Prefetch Scheduler          │
│  • tgcrypto C-Accelerated Packet Decryption (>80 MB/s) │
└───────────┬────────────────────────────────────────────┘
            │ Parallel MTProto Requests (512 KB chunks)
            ▼
┌────────────────────────┐
│ Telegram Cloud Servers │
└────────────────────────┘
```

1. **Instant RAM Seek (`BlockCache`)**: Video players probe the first 64 KB (header) and last 512 KB (atom indexes) before requesting byte 0. Teleflux caches these segments in RAM, ensuring startup playback in **under 300 ms**.
2. **3-Priority Prefetch Scheduler**: Urgent playback head requests receive top priority, near read-ahead buffers ensure uninterrupted playback, and speculative prefetch downloads future chunks in the background. Network pauses between packets are completely eliminated.
3. **HTTP 206 Standard**: Fully compatible with ExoPlayer (Android TV), MPV (Desktop), and AVPlayer (Apple).

---

## ✍️ Reply-Based Manual Title Correction

If an obscure or poorly formatted filename in Telegram is misidentified by automatic extraction, you can correct it instantly using Telegram replies:

1. **Reply in Telegram**: In your connected Telegram channel (or its linked discussion group), reply directly to the video message with the correct title hint (e.g. `Naalaam Mura` or `Breaking Bad`).
2. **Fuzzy TMDB Validation**: Teleflux searches TMDB and uses intelligent title similarity matching to resolve spelling variations, transliterations, and typos (e.g. `Naalaam Mura` -> `Naalam Mura (2022)`). If no verified match is found on TMDB, the query is rejected rather than blindly accepted.
3. **Series-Wide Propagation**: If you reply to a single episode of a TV show, Teleflux applies the canonical TMDB title and series metadata across **all matching episodes** while strictly preserving each episode's season and episode numbers. Unrelated shows are never affected.
4. **Future Episodes & Rescans**: Successful corrections are permanently stored in SQLite (`title_corrections`). Future episodes of the series automatically inherit the canonical metadata, and corrections take priority over automatic parsing during channel rescans or container restarts.
5. **Confirmation**: If the bot has message permissions, it sends a confirmation reply (e.g. `✅ Corrected to Naalam Mura (2022) [TMDB: 1032906]`).
6. **Editing Replies (Typo Correction)**: If you accidentally make a typo or provide the wrong hint in your reply, simply edit your reply message in Telegram. Teleflux listens for message edits (`events.MessageEdited`), rechecks TMDB with the updated title hint, remodifies the video (or all matching TV episodes) with the exact canonical match, and updates its previous confirmation response (e.g. `🔄 Updated to Naalam Mura (2022)...`).

---

## 🔞 Optional: Adult Mode (Teleflux Red)

Teleflux includes an optional toggle for adult media collections powered by StashDB:

- **How to enable:**
  1. In your `.env` file, set:
     ```env
     ADULT_ONLY=True
     STASHDB_API_KEY=your_stashdb_api_key_here
     ```
  2. (Optional) Get a free API key at [stashdb.org](https://stashdb.org/).
  3. Restart Teleflux (`docker compose restart` or restart Python).
- When enabled, the addon activates StashDB GraphQL scene matching and presents your collection under the `Teleflux Red` catalog. When disabled (`ADULT_ONLY=False`, default), Teleflux operates purely in mainstream Movies & TV Series mode with TMDB.

---

## 💡 Inspirations & References

Teleflux was inspired by and built with reference to the work of these open-source projects:

- **[TGTV](https://github.com/aravindbuilds/TGTV)** by [@aravindbuilds](https://github.com/aravindbuilds) — Pioneered concepts for bridging Telegram channel media into a modern TV / media-center experience.
- **[TMPlayer](https://github.com/dracu-lah/TMPlayer)** by [@dracu-lah](https://github.com/dracu-lah) — Outstanding reference for Telegram MTProto media streaming and direct playback architectures.

---

## ❓ Troubleshooting & FAQ

### Why are older channel videos not showing up automatically?
In Telegram's MTProto architecture, **Bots cannot read message history created before they joined the channel**.
- **The 5-Second Fix:** Open your Telegram channel on mobile or desktop, select your older video messages, and click **Forward** directly back into the channel (or to your bot's private chat). Telegram creates a new event with the current timestamp, and Teleflux will instantly index and match the metadata.
- **Scan Channel History Button:** You can also hit **🔄 Scan Channel History Now** on the `/stremio` dashboard to pull any history accessible to the bot.

### "Failed to fetch manifest" error in Stremio / Nuvio
Ensure `PUBLIC_URL` in `.env` is set to your server's reachable LAN IP or public domain (e.g. `http://192.168.1.16:7001`) instead of `127.0.0.1` or `localhost`. Devices on your Wi-Fi cannot resolve `localhost` back to your server.

### How do I rotate my security token if compromised?
On the `/stremio` dashboard, click **🔄 Rotate Secret Key**. Teleflux will instantly generate a new 32-character cryptographic key, invalidate the previous URL, and provide you with the updated manifest link.

### What is the maximum video file size supported?
Telegram allows standard bot accounts to stream files up to **2 GB** each. Telegram Premium accounts support files up to **4 GB**.

---

## 📄 License

This project is open-sourced under the [MIT License](LICENSE).

---

<div align="center">
  <sub>Built for self-hosters and media enthusiasts. Teleflux is not affiliated with Telegram, Stremio, or Nuvio.</sub>
</div>
