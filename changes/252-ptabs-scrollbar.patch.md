- **Pipeline pages: no stray vertical scrollbar beside the tabs.** The tab
  bar scrolls sideways on a narrow screen (`overflow-x: auto`), which made
  its vertical overflow `auto` as well, and the tabs' underline overlapping
  the border by 1px then showed a vertical scrollbar. It is now hidden; the
  keyboard focus ring sits inside the tab so it isn't clipped.
