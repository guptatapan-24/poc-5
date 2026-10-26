"use client";

import dynamic from "next/dynamic";

const App = dynamic(() => import("../App"), {
  ssr: false,
  loading: () => (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        minHeight: "100vh",
        background: "#090d16",
        color: "#94a3b8",
        fontFamily: "'Plus Jakarta Sans', system-ui, sans-serif",
        fontSize: "1.1rem",
        letterSpacing: "0.02em",
      }}
    >
      Loading Next.js Real-Time Stock Dashboard...
    </div>
  ),
});

export default function Page() {
  return <App />;
}
