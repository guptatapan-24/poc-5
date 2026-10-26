import "../index.css";

export const metadata = {
  title: "Real-Time Stock Event Capture & Replay System | Kafka & WebSockets",
  description:
    "Real-Time Stock Event Capture and Replay System built with Apache Kafka, Django Channels, Redis, WebSockets, and Next.js.",
  icons: {
    icon: "/favicon.svg",
  },
};

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <head>
        <link rel="icon" type="image/svg+xml" href="/favicon.svg" />
      </head>
      <body>{children}</body>
    </html>
  );
}
