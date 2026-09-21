// Render Material icons to PNG in the brand colours, once, into icons/.
const React = require("react");
const { renderToStaticMarkup } = require("react-dom/server");
const sharp = require("sharp");
const md = require("react-icons/md");
const fs = require("fs");

const ICONS = {
  storage:     md.MdStorage,
  history:     md.MdHistory,
  book:        md.MdMenuBook,
  graph:       md.MdHub,
  snapshot:    md.MdLayers,
  model:       md.MdAutoAwesome,
  chat:        md.MdForum,
  verified:    md.MdVerified,
  fingerprint: md.MdFingerprint,
  factcheck:   md.MdFactCheck,
  publish:     md.MdPublish,
  lock:        md.MdLock,
  analyst:     md.MdPersonSearch,
  leader:      md.MdInsights,
  newcomer:    md.MdSchool,
  steward:     md.MdVerifiedUser,
  wiki:        md.MdDescription,
  chartnote:   md.MdInsertChartOutlined,
  block:       md.MdBlock,
  help:        md.MdHelpOutline,
  science:     md.MdScience,
  compile:     md.MdMemory,
  people:      md.MdGroups,
  clock:       md.MdOutlineTimer,
};

const COLOURS = { blue: "#016FD0", white: "#FFFFFF", navy: "#002663", amber: "#B26A00" };

fs.mkdirSync("icons", { recursive: true });

(async () => {
  let n = 0;
  for (const [name, Comp] of Object.entries(ICONS)) {
    if (typeof Comp !== "function") {
      console.error("MISSING ICON:", name);
      continue;
    }
    for (const [cname, hex] of Object.entries(COLOURS)) {
      const svg = renderToStaticMarkup(
        React.createElement(Comp, { color: hex, size: 256 })
      );
      await sharp(Buffer.from(svg), { density: 600 })
        .resize(256, 256, { fit: "contain", background: { r: 0, g: 0, b: 0, alpha: 0 } })
        .png()
        .toFile(`icons/${name}-${cname}.png`);
      n++;
    }
  }
  console.log("wrote", n, "icons");
})();
