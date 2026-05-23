/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        ink: "#0f172a", // darker modern blue-gray
        shell: "#f8fafc", // light modern gray for background
        accent: "#3b82f6", // modern blue accent
        moss: "#10b981", // emerald green
        mist: "#e2e8f0", // slate light
      },
      fontFamily: {
        display: ["Outfit", "sans-serif"],
        body: ["Inter", "sans-serif"],
      },
      boxShadow: {
        panel: "0 4px 20px rgba(0, 0, 0, 0.05)",
      },
    },
  },
  plugins: [],
};
