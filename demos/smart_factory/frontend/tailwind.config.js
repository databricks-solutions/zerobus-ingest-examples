/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        factory: {
          bg: "#0a0e17",
          card: "#111827",
          border: "#1f2937",
          accent: "#3b82f6",
        },
      },
      animation: {
        "pulse-slow": "pulse 3s cubic-bezier(0.4, 0, 0.6, 1) infinite",
        "glow-green": "glow-green 2s ease-in-out infinite alternate",
        "glow-amber": "glow-amber 1s ease-in-out infinite alternate",
        "glow-red": "glow-red 0.5s ease-in-out infinite alternate",
      },
      keyframes: {
        "glow-green": {
          "0%": { boxShadow: "0 0 5px #10b981, 0 0 10px #10b98133" },
          "100%": { boxShadow: "0 0 10px #10b981, 0 0 20px #10b98155" },
        },
        "glow-amber": {
          "0%": { boxShadow: "0 0 5px #f59e0b, 0 0 10px #f59e0b33" },
          "100%": { boxShadow: "0 0 15px #f59e0b, 0 0 30px #f59e0b55" },
        },
        "glow-red": {
          "0%": { boxShadow: "0 0 5px #ef4444, 0 0 10px #ef444433" },
          "100%": { boxShadow: "0 0 20px #ef4444, 0 0 40px #ef444466" },
        },
      },
    },
  },
  plugins: [],
};
