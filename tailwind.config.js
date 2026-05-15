/** @type {import('tailwindcss').Config} */
module.exports = {
  // Scan templates AND the renderer's helpers — some Tailwind class
  // strings (state-chip background/text colours) are returned from Go
  // funcmap helpers and would be invisible to a templates-only scan.
  content: [
    './internal/templates/templates/**/*.html',
    './internal/templates/*.go',
  ],
  darkMode: 'class',
  theme: {
    extend: {},
  },
  plugins: [],
};
