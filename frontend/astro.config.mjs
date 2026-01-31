// @ts-check
import { defineConfig } from "astro/config";

import node from "@astrojs/node";

// https://astro.build/config
export default defineConfig({
  build: {
      concurrency: 4,
  },

  server: {
      host: true,
  },

  adapter: node({
    mode: "standalone",
  }),
});