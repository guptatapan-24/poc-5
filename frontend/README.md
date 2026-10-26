# Next.js Stock Event Dashboard

Real-time stock price streaming, threshold alert management, Kafka event replay, and paper trade simulator interface built with **Next.js (App Router)**, **React 19**, **Lucide Icons**, and native WebSockets.

## Getting Started

1. Install dependencies:
   ```bash
   npm install
   ```

2. Run development server (on port 3000):
   ```bash
   npm run dev
   ```

3. Build production bundle:
   ```bash
   npm run build
   ```

4. Start production server:
   ```bash
   npm run start
   ```

## Architecture

- `src/app/layout.jsx`: Root layout configuring SEO metadata, responsive viewport, and global design tokens.
- `src/app/page.jsx`: Dashboard root page utilizing Next.js dynamic client hydration for real-time WebSocket connectivity.
- `src/components/`: Modular UI widgets (Header, EventFeed, ReplayControls, SimulatorControls, StockPriceDisplay, ThresholdConfig, Modals).
- `src/hooks/useWebSocket.js`: SSR-safe WebSocket hook handling live Kafka push events, catch-up synchronization, and connection lifecycle.
- `src/services/api.js`: REST client for Django backend API.
