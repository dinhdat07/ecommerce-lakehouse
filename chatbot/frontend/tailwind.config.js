/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        ink: "#10203a",
        shell: "#f4f0e8",
        accent: "#d76831",
        moss: "#6b7c58",
        mist: "#dbe4ea",
      },
      fontFamily: {
        display: ["Georgia", "ui-serif", "serif"],
        body: ["'Segoe UI'", "system-ui", "sans-serif"],
      },
      boxShadow: {
        panel: "0 18px 40px rgba(16, 32, 58, 0.08)",
      },
    },
  },
  plugins: [],
};
