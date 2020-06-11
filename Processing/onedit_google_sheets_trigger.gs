function onEdit(e) {
    var sh = e.source.getActiveSheet();
    if (sh.getName() !== 'tv_shows' || e.range.columnStart < 10 || e.range.columnStart > 11 || e.range.rowStart < 2) return;
    sh.getRange(e.range.rowStart, 12).setValue(new Date())
}
