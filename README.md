# 🚩 VARI Sahayak (वारी सहायक)

> **Offline-First Emergency Triage & Field Coordination Platform for the Pandharpur Wari**

[![Kotlin](https://img.shields.io/badge/Language-Kotlin-7F52FF?style=flat-square&logo=kotlin&logoColor=white)](https://kotlinlang.org/)
[![Android](https://img.shields.io/badge/Platform-Android_API_23+-3DDC84?style=flat-square&logo=android&logoColor=white)](https://developer.android.com/)
[![Supabase](https://img.shields.io/badge/Backend-Supabase-3ECF8E?style=flat-square&logo=supabase&logoColor=white)](https://supabase.com/)
[![Jetpack Compose](https://img.shields.io/badge/UI-Jetpack_Compose-4285F4?style=flat-square&logo=jetpackcompose&logoColor=white)](https://developer.android.com/jetpack/compose)

---

## 📌 Overview

**VARI Sahayak** is a purpose-built operational platform designed to ensure fast, reliable emergency response during the **Pandharpur Wari**—one of the world's largest annual walking pilgrimages involving hundreds of thousands of Varkaris. 

In high-density crowd environments with intermittent cellular connectivity, standard communications fail. VARI Sahayak connects field volunteers, medical responders, police personnel, NGOs, and command centers through an offline-resilient Android client and cloud infrastructure.

---

## ✨ Key Features

- 📶 **Offline-First Resilience**: Incidents are saved locally immediately to Room DB with unique client GUIDs and queued for WorkManager background sync once connectivity returns. Zero report data loss.
- 🆘 **SOS Bridge (QR Tagging)**: Enables phone-less pilgrims to carry non-sensitive QR tags. Volunteers scan these tags to instantly file emergency requests tied to the pilgrim's identifier.
- 🤖 **AI Triage with Guardrails**: Powered by **Google Gemini** for intelligent incident classification, priority scoring, and summary generation—strictly bounded by deterministic safety rules (AI never overrides emergency SOS signals).
- 🔍 **Facial Recognition Engine**: Microservice leveraging OpenCV and deep face embeddings (`pgvector`) to rapidly locate and identify missing pilgrims across crowd photos.
- 🗺️ **Command Dispatch & Realtime Triage**: Real-time responder matching based on role, location proximity, active workload, and assigned sector, backed by LiveKit audio dispatch.

---

## 🏗️ System Architecture

```
📱 Android Client (Kotlin / Compose / Room)
   │ 
   ├── 💾 Room DB (Single Source of Truth)
   └── 🔄 WorkManager (Idempotent Sync Engine)
           │
           ▼
⚡ Supabase Cloud Backend (PostgreSQL + RLS)
   │
   ├── ⚡ Edge Functions (Responder Match & Dispatch)
   ├── 🧠 Gemini AI API (Intelligent Triage & Summarization)
   ├── 👤 Face Matching Service (FastAPI + pgvector)
   └── 🎙️ LiveKit Voice Dispatch (Audio Channels)
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| **Android Client** | Kotlin, Jetpack Compose (Material 3), Coroutines, Flow, Hilt (DI), Room DB, WorkManager |
| **Location & Media** | Google Maps SDK, ML Kit Barcode Scanning, CameraX |
| **Backend & Database** | Supabase (PostgreSQL, Row Level Security, Realtime Sync, Auth) |
| **Edge & AI** | Supabase Edge Functions (Deno/TypeScript), Google Gemini API |
| **Microservices** | Python FastAPI (InsightFace / ResNet / OpenCV), LiveKit WebRTC |

---

## 📁 Repository Structure

```
.
├── app/                  # Native Android application (Jetpack Compose + Hilt + Room)
├── supabase/             # Database migrations, RLS policies, & Edge Functions
├── services/
│   ├── face-matching/    # Python microservice for missing person facial search
│   └── livekit/          # Voice coordination & audio room token dispatch
├── public-site/          # Web-based Command Center & Dashboard UI
├── Project Summary.md    # Product Requirements & Architecture Specs
└── setup.md              # Detailed local setup and environment configuration guide
```

---

## 🚀 Quick Start

### 1. Prerequisites
- **JDK 17+** & **Android Studio** (API level 37 SDK / Build Tools 36.0.0)
- **Node.js 18+** & **Supabase CLI**

### 2. Environment Setup
Clone the repository and copy the environment template:
```bash
cp .env.example .env
```
Fill in the client configuration values in `.env`:
```properties
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_ANON_KEY=your-supabase-anon-key
GOOGLE_MAPS_API_KEY=your-google-maps-api-key
```

### 3. Build & Run
Open the root directory in **Android Studio**, sync Gradle project, and run on an Android emulator or device (API 26+ recommended).

> 📖 For complete backend, Firebase, and microservice setup instructions, refer to **[setup.md](file:///c:/Users/victus/Documents/Hackathons/VariSahayak/setup.md)**.

---

## 📄 License & Status

Developed for the **Pandharpur Wari Field Operations**. Documented and specified under the baseline MVP contract.
