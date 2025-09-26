// @ts-check
import { defineConfig } from "astro/config";
import preact from "@astrojs/preact";

// https://astro.build/config
export default defineConfig({
    build: {
        concurrency: 4,
    },
    server: {
        host: true,
    },
    integrations: [preact({ compat: true })],
});
