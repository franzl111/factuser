#!/usr/bin/env python3
"""
PDF Sorter — resilient version (EU Document AI) with:
- Robust open/render: prompt for password if encrypted, quarantine corrupt PDFs, never hangs
- Global shortcuts: Shift+↓/↑ (next/prev file), PgDn/PgUp (page)
- Mouse edge flip (wheel at bottom/top changes page)
- Page toolbar (Prev/Next, live page indicator)
- Move buttons (ALL/D5/FL) write CSV & auto-next + "Quarantine & Next"
- CSV: ~/Nextcloud/COLLECTED/factus_2021-2024_manual.csv
- Autocomplete from CSV (Provider/TaxID/IBAN)
- AI Suggestions (DocAI EU), batch cache (ai_cache.csv); dropdown order: current value, leave, clear, then candidates
- Click a word on the PDF to fill fields (Provider/Invoice/Total/TaxID/IBAN/Date)
- Log pane: errors, AI, CSV, batch

Run:
  python pdf_sorter.py /path/to/folder

Dependencies:
  pip install PySide6 PyMuPDF google-cloud-documentai
"""

import sys, os, re, csv, shutil, subprocess
from pathlib import Path
from typing import Optional, List, Dict, Tuple

from PySide6 import QtCore, QtGui, QtWidgets
import fitz  # PyMuPDF

# ---- Google Document AI ----
try:
    from google.cloud import documentai as docai
    from google.api_core.client_options import ClientOptions
    _HAVE_DOCAI = True
except Exception:
    _HAVE_DOCAI = False

HOME = Path.home()
BASE_DEST = HOME / "Nextcloud" / "COLLECTED"
ALL_DIR   = BASE_DEST / "ALL"
D5_DIR    = BASE_DEST / "D5"
FL_DIR    = BASE_DEST / "FL"
BROKEN_DIR= BASE_DEST / "BROKEN"
CSV_PATH  = BASE_DEST / "factus_2021-2024_manual.csv"
AI_CACHE_CSV = BASE_DEST / "ai_cache.csv"

# Credentials & processors (EU)
SERVICE_ACCOUNT_JSON = os.path.expanduser("~/Documents/Json-key/ocr.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_JSON
PROJECT_ID = "ocr-data-on-drive"
LOCATION   = "eu"
INVOICE_PROCESSOR_ID = "fbe60c7caa34bd2b"
EXPENSE_PROCESSOR_ID = "3e58df9f3e32dcc6"

LEAVE_UNCHANGED = "— leave unchanged —"
CLEAR_FIELD     = "— clear field —"

def get_docai_client():
    if not _HAVE_DOCAI:
        return None
    api_endpoint = f"{LOCATION}-documentai.googleapis.com"
    return docai.DocumentProcessorServiceClient(
        client_options=ClientOptions(api_endpoint=api_endpoint)
    )

# ----- Date parsing -----
MONTHS = {
    'jan':1,'january':1,'feb':2,'february':2,'mar':3,'march':3,'apr':4,'april':4,'may':5,
    'jun':6,'june':6,'jul':7,'july':7,'aug':8,'august':8,'sep':9,'sept':9,'september':9,
    'oct':10,'october':10,'nov':11,'november':11,'dec':12,'december':12,
}
def parse_date_any(s: str) -> Optional[Tuple[int,int,int]]:
    s0 = (s or "").strip()
    if not s0:
        return None
    s1 = s0.replace(',', ' ').strip()

    m = re.match(r'^\s*(\d{4})[.\-\/](\d{1,2})[.\-\/](\d{1,2})\s*$', s1)
    if m: return (int(m.group(1)), int(m.group(2)), int(m.group(3)))

    m = re.match(r'^\s*(\d{1,2})[.\-\/](\d{1,2})[.\-\/](\d{2,4})\s*$', s1)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if y < 100: y += 2000
        return (y, mo, d)

    m = re.match(r'^\s*(\d{1,2})\s+([A-Za-z\.]+)\s+(\d{2,4})\s*$', s1)
    if m:
        d = int(m.group(1)); mon = m.group(2).lower().strip('.'); y = int(m.group(3))
        if y < 100: y += 2000
        if mon in MONTHS: return (y, MONTHS[mon], d)

    m = re.match(r'^\s*([A-Za-z\.]+)\s+(\d{1,2})(?:,)?\s+(\d{2,4})\s*$', s1)
    if m:
        mon = m.group(1).lower().strip('.'); d = int(m.group(2)); y = int(m.group(3))
        if y < 100: y += 2000
        if mon in MONTHS: return (y, MONTHS[mon], d)

    m = re.match(r'^\s*(\d{1,2})[\/](\d{1,2})[\/](\d{2,4})\s*$', s1)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if y < 100: y += 2000
        return (y, mo, d)
    return None

# ----- Clickable PDF label -----
class ClickableLabel(QtWidgets.QLabel):
    clickedAt = QtCore.Signal(int, int)
    def mousePressEvent(self, e: QtGui.QMouseEvent):
        if self.pixmap() and e.button() == QtCore.Qt.LeftButton:
            p = e.position().toPoint()
            self.clickedAt.emit(p.x(), p.y())
        super().mousePressEvent(e)

class PdfSorter(QtWidgets.QMainWindow):
    def __init__(self, start_dir: Optional[Path] = None):
        super().__init__()
        self.setWindowTitle("PDF Sorter")
        self.resize(1700, 1040)

        # State
        self.files: List[Path] = []
        self.idx: int = 0
        self.page: int = 0
        self.zoom: float = 1.5
        self.ai_cache: Dict[str, Dict[str, List[str]]] = {}
        self._passwords: Dict[str, str] = {}   # filename -> password (cached this session)

        # Viewer & toolbar
        self.label = ClickableLabel("No PDF loaded")
        self.label.setAlignment(QtCore.Qt.AlignCenter)
        self.label.clickedAt.connect(self.on_pdf_click)
        self.scroll = QtWidgets.QScrollArea()
        self.scroll.setWidgetResizable(True)
        self.scroll.setWidget(self.label)

        self.page_info = QtWidgets.QLabel("page —/—")
        btn_prev = QtWidgets.QPushButton("◀ Prev Page"); btn_prev.clicked.connect(lambda: self.change_page(-1))
        btn_next = QtWidgets.QPushButton("Next Page ▶"); btn_next.clicked.connect(lambda: self.change_page(1))
        page_bar = QtWidgets.QHBoxLayout()
        page_bar.addWidget(btn_prev); page_bar.addWidget(btn_next); page_bar.addStretch(); page_bar.addWidget(self.page_info)
        page_wrap = QtWidgets.QWidget(); page_wrap.setLayout(page_bar)
        left_layout = QtWidgets.QVBoxLayout()
        left_layout.addWidget(page_wrap); left_layout.addWidget(self.scroll)
        left_wrap = QtWidgets.QWidget(); left_wrap.setLayout(left_layout)

        # Metadata form
        form_wrap = QtWidgets.QWidget(); form = QtWidgets.QFormLayout(form_wrap)
        self.year_edit = QtWidgets.QLineEdit(); self.year_edit.setPlaceholderText("YYYY")
        self.month_edit = QtWidgets.QLineEdit(); self.month_edit.setPlaceholderText("MM")
        self.day_edit = QtWidgets.QLineEdit(); self.day_edit.setPlaceholderText("DD")
        self.provider_edit = QtWidgets.QLineEdit(); self.provider_edit.setPlaceholderText("Provider")
        self.invoice_edit = QtWidgets.QLineEdit(); self.invoice_edit.setPlaceholderText("Invoice #")
        self.total_edit = QtWidgets.QLineEdit(); self.total_edit.setPlaceholderText("Total")
        self.card_edit = QtWidgets.QLineEdit(); self.card_edit.setPlaceholderText("Card")
        self.taxid_edit = QtWidgets.QLineEdit(); self.taxid_edit.setPlaceholderText("Tax ID")
        self.iban_edit = QtWidgets.QLineEdit(); self.iban_edit.setPlaceholderText("IBAN")
        form.addRow("Year", self.year_edit); form.addRow("Month", self.month_edit); form.addRow("Day", self.day_edit)
        form.addRow("Provider", self.provider_edit); form.addRow("Invoice", self.invoice_edit); form.addRow("Total", self.total_edit)
        form.addRow("Card", self.card_edit); form.addRow("Tax ID", self.taxid_edit); form.addRow("IBAN", self.iban_edit)

        # Autocomplete models / completers
        self.provider_model = QtCore.QStringListModel()
        self.taxid_model    = QtCore.QStringListModel()
        self.iban_model     = QtCore.QStringListModel()
        for edit, model in [(self.provider_edit,self.provider_model),
                            (self.taxid_edit,self.taxid_model),
                            (self.iban_edit,self.iban_model)]:
            comp = QtWidgets.QCompleter(model); comp.setCaseSensitivity(QtCore.Qt.CaseInsensitive)
            edit.setCompleter(comp)

        # Auto-advance Y->M->D
        self.year_edit.textChanged.connect(self._adv_year)
        self.month_edit.textChanged.connect(self._adv_month)
        self.day_edit.textChanged.connect(self._adv_day)

        # Move & Skip & Quarantine
        move_btns = QtWidgets.QHBoxLayout()
        self.btn_move_all = QtWidgets.QPushButton("Move to ALL"); self.btn_move_all.clicked.connect(lambda: self.move_to_dir(ALL_DIR))
        self.btn_move_d5  = QtWidgets.QPushButton("Move to D5");  self.btn_move_d5.clicked.connect(lambda: self.move_to_dir(D5_DIR))
        self.btn_move_fl  = QtWidgets.QPushButton("Move to FL");  self.btn_move_fl.clicked.connect(lambda: self.move_to_dir(FL_DIR))
        self.btn_skip     = QtWidgets.QPushButton("Skip & Next"); self.btn_skip.clicked.connect(self.next_file)
        self.btn_quar     = QtWidgets.QPushButton("Quarantine & Next"); self.btn_quar.clicked.connect(self.quarantine_and_next)
        for b in (self.btn_move_all, self.btn_move_d5, self.btn_move_fl): move_btns.addWidget(b)
        move_btns.addWidget(self.btn_quar)
        move_btns.addStretch(); move_btns.addWidget(self.btn_skip)
        move_box = QtWidgets.QWidget(); move_box.setLayout(move_btns)

        # AI Suggestions + buttons
        self.sugg_fields: Dict[str, QtWidgets.QComboBox] = {}
        ai_group = QtWidgets.QGroupBox("AI Suggestions")
        ai_layout = QtWidgets.QFormLayout(ai_group)
        for key, label in [("date","Date"),("provider","Provider"),("invoice","Invoice #"),("total","Total"),("taxid","Tax ID"),("iban","IBAN")]:
            combo = QtWidgets.QComboBox(); combo.setEditable(False)
            combo.addItem(LEAVE_UNCHANGED); combo.addItem(CLEAR_FIELD)
            ai_layout.addRow(label, combo)
            self.sugg_fields[key] = combo

        self.btn_ai_invoice = QtWidgets.QPushButton("Run Invoice AI");  self.btn_ai_invoice.clicked.connect(lambda: self.run_ai(INVOICE_PROCESSOR_ID))
        self.btn_ai_expense = QtWidgets.QPushButton("Run Expense AI");  self.btn_ai_expense.clicked.connect(lambda: self.run_ai(EXPENSE_PROCESSOR_ID))
        self.btn_ai_batch   = QtWidgets.QPushButton("Batch AI (cache)"); self.btn_ai_batch.clicked.connect(self.batch_ai_cache)
        self.btn_apply      = QtWidgets.QPushButton("Apply Selected");   self.btn_apply.clicked.connect(self.apply_suggestions)
        ai_buttons_row = QtWidgets.QHBoxLayout()
        for b in (self.btn_ai_invoice,self.btn_ai_expense,self.btn_ai_batch,self.btn_apply): ai_buttons_row.addWidget(b)
        ai_buttons_box = QtWidgets.QWidget(); ai_buttons_box.setLayout(ai_buttons_row)
        ai_layout.addRow(ai_buttons_box)

        # Log
        self.log = QtWidgets.QTextEdit(); self.log.setReadOnly(True)
        log_group = QtWidgets.QGroupBox("Log"); log_layout = QtWidgets.QVBoxLayout(log_group); log_layout.addWidget(self.log)

        # Right side
        right = QtWidgets.QVBoxLayout()
        right.addWidget(form_wrap); right.addWidget(move_box); right.addWidget(ai_group); right.addWidget(log_group)
        right_wrap = QtWidgets.QWidget(); right_wrap.setLayout(right)

        # Splitter
        splitter = QtWidgets.QSplitter()
        splitter.addWidget(left_wrap); splitter.addWidget(right_wrap)
        splitter.setStretchFactor(0, 3); splitter.setStretchFactor(1, 1)
        self.setCentralWidget(splitter)

        # Global shortcuts
        self.sc_next  = QtGui.QShortcut(QtGui.QKeySequence("Shift+Down"), self, activated=self.next_file)
        self.sc_prev  = QtGui.QShortcut(QtGui.QKeySequence("Shift+Up"),   self, activated=self.prev_file)
        self.sc_pgdn  = QtGui.QShortcut(QtGui.QKeySequence(QtCore.Qt.Key_PageDown), self, activated=lambda: self.change_page(1))
        self.sc_pgup  = QtGui.QShortcut(QtGui.QKeySequence(QtCore.Qt.Key_PageUp),   self, activated=lambda: self.change_page(-1))
        for sc in (self.sc_next,self.sc_prev,self.sc_pgdn,self.sc_pgup): sc.setContext(QtCore.Qt.ApplicationShortcut)

        # Load CSV autocomplete & AI cache
        self._load_csv_suggestions()
        self._load_ai_cache()

        # Initial folder
        if start_dir and Path(start_dir).is_dir():
            self.load_dir(Path(start_dir))

        # Wheel-based page flip at edges
        self.scroll.installEventFilter(self)

    # -------- Logging --------
    def log_msg(self, msg: str):
        self.log.append(msg)
        print(msg)

    # -------- Folder / Rendering --------
    def load_dir(self, folder: Path):
        self.files = sorted([p for p in folder.glob("*.pdf")])
        if self.files:
            self.idx = 0; self.page = 0
            self.render_current()
        else:
            self.label.setText("No PDFs found")
            self.setWindowTitle("PDF Sorter")

    def try_open_doc(self, path: Path) -> Optional[fitz.Document]:
        """Open a PDF, prompt for password if needed, return doc or None."""
        try:
            doc = fitz.open(path)
            if doc.needs_pass:
                pwd = self._passwords.get(path.name, "")
                if pwd:
                    if doc.authenticate(pwd):
                        return doc
                # prompt user once
                pwd, ok = QtWidgets.QInputDialog.getText(self, "Password required",
                                                         f"Enter password for:\n{path.name}",
                                                         QtWidgets.QLineEdit.Password)
                if ok and pwd:
                    if doc.authenticate(pwd):
                        self._passwords[path.name] = pwd
                        return doc
                # failed or canceled
                self.log_msg(f"Encrypted PDF skipped: {path.name}")
                doc.close()
                return None
            return doc
        except Exception as e:
            self.log_msg(f"Open error: {path.name} → {e}")
            return None

    def render_current(self):
        if not self.files:
            self.label.setText("No PDFs found"); return
        path = self.files[self.idx]
        doc = self.try_open_doc(path)
        if doc is None:
            # cannot open; quarantine & next
            self.quarantine_and_next()
            return
        try:
            self.page = max(0, min(self.page, doc.page_count-1))
            pg = doc.load_page(self.page)
            pix = pg.get_pixmap(matrix=fitz.Matrix(self.zoom,self.zoom))
            img = QtGui.QImage(pix.samples, pix.width, pix.height, pix.stride, QtGui.QImage.Format_RGB888)
            pm = QtGui.QPixmap.fromImage(img)
            self.label.setPixmap(pm)
            self.page_info.setText(f"page {self.page+1}/{doc.page_count}")
            self.setWindowTitle(f"{path.name} — page {self.page+1}/{doc.page_count}")
            doc.close()
            # populate suggestions from cache with current value first
            self.populate_ai_from_cache(path.name)
        except Exception as e:
            self.log_msg(f"Render error: {path.name} → {e}")
            try: doc.close()
            except: pass
            self.quarantine_and_next()

    # -------- Navigation --------
    def next_file(self):
        if not self.files: return
        self.idx = (self.idx + 1) % len(self.files); self.page = 0; self.render_current()
    def prev_file(self):
        if not self.files: return
        self.idx = (self.idx - 1) % len(self.files); self.page = 0; self.render_current()
    def change_page(self, delta: int):
        if not self.files: return
        self.page = max(0, self.page + delta); self.render_current()
    def eventFilter(self, obj, event):
        if obj is self.scroll and event.type() == QtCore.QEvent.Type.Wheel:
            bar = self.scroll.verticalScrollBar()
            at_top = bar.value() <= bar.minimum()
            at_bottom = bar.value() >= bar.maximum()
            dy = event.angleDelta().y()
            if dy < 0 and at_bottom: self.change_page(1); return True
            if dy > 0 and at_top:    self.change_page(-1); return True
        return super().eventFilter(obj, event)

    # -------- Auto-advance date fields --------
    def _adv_year(self, txt: str):
        if len(txt) == 4: self.month_edit.setFocus(); self.month_edit.selectAll()
    def _adv_month(self, txt: str):
        if len(txt) == 2: self.day_edit.setFocus(); self.day_edit.selectAll()
    def _adv_day(self, txt: str):
        if len(txt) == 2: self.provider_edit.setFocus(); self.provider_edit.selectAll()

    # -------- Move, Quarantine & CSV --------
    def move_to_dir(self, dest_dir: Path):
        if not self.files: return
        src = self.files[self.idx]
        try:
            dest_dir.mkdir(parents=True, exist_ok=True)
            target = dest_dir / src.name
            if target.exists():
                stem, ext = src.stem, src.suffix; i=1
                while (dest_dir/f"{stem}({i}){ext}").exists(): i+=1
                target = dest_dir/f"{stem}({i}){ext}"
            shutil.move(str(src), str(target))
            self.log_msg(f"Moved {src} → {target}")
            self._append_csv(target)
        except Exception as e:
            self.log_msg(f"Move error: {e}")
            QtWidgets.QMessageBox.critical(self, "Move failed", str(e))
            return
        self.files.pop(self.idx)
        if self.files:
            if self.idx >= len(self.files): self.idx = 0
            self.page = 0; self.render_current()
        else:
            self.label.setText("No PDFs left"); self.setWindowTitle("PDF Sorter")

    def quarantine_and_next(self):
        """Move the current bad file to BROKEN and advance."""
        if not self.files: return
        src = self.files[self.idx]
        try:
            BROKEN_DIR.mkdir(parents=True, exist_ok=True)
            target = BROKEN_DIR / src.name
            if src.exists():
                shutil.move(str(src), str(target))
                self.log_msg(f"Quarantined {src} → {target}")
        except Exception as e:
            self.log_msg(f"Quarantine error: {e}")
        # advance
        self.files.pop(self.idx)
        if self.files:
            if self.idx >= len(self.files): self.idx = 0
            self.page = 0; self.render_current()
        else:
            self.label.setText("No PDFs left"); self.setWindowTitle("PDF Sorter")

    def _append_csv(self, filename: Path):
        BASE_DEST.mkdir(parents=True, exist_ok=True)
        new_file = not CSV_PATH.exists()
        try:
            with CSV_PATH.open('a', newline='', encoding='utf-8') as f:
                w = csv.writer(f)
                if new_file:
                    w.writerow(["filename","year","month","day","provider","invoice","total","card","taxid","iban"])
                w.writerow([
                    filename.name,
                    self.year_edit.text().strip(), self.month_edit.text().strip(), self.day_edit.text().strip(),
                    self.provider_edit.text().strip(), self.invoice_edit.text().strip(), self.total_edit.text().strip(),
                    self.card_edit.text().strip(), self.taxid_edit.text().strip(), self.iban_edit.text().strip()
                ])
            self.log_msg(f"CSV updated for {filename}")
        except Exception as e:
            self.log_msg(f"CSV write error: {e}")
            QtWidgets.QMessageBox.warning(self, "CSV", f"Failed to write CSV: {e}")
        self._load_csv_suggestions()

    def _load_csv_suggestions(self):
        providers, taxids, ibans = set(), set(), set()
        if CSV_PATH.exists():
            try:
                with CSV_PATH.open('r', newline='', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        p = (row.get('provider') or '').strip()
                        t = (row.get('taxid') or '').strip()
                        i = (row.get('iban') or '').strip()
                        if p:
                            providers.add(p)
                        if t:
                            taxids.add(t)
                        if i:
                            ibans.add(i)
            except Exception as e:
                self.log_msg(f"CSV read error: {e}")
        self.provider_model.setStringList(sorted(providers))
        self.taxid_model.setStringList(sorted(taxids))
        self.iban_model.setStringList(sorted(ibans))

    # -------- AI cache (batch) --------
    def _load_ai_cache(self):
        self.ai_cache.clear()
        if not AI_CACHE_CSV.exists(): return
        try:
            with AI_CACHE_CSV.open('r', newline='', encoding='utf-8') as f:
                r = csv.DictReader(f)
                for row in r:
                    name = row.get('filename') or ''
                    if not name: continue
                    entry = {}
                    for key in ('date','provider','invoice','total','taxid','iban'):
                        vals = (row.get(key) or '').strip()
                        entry[key] = [v.strip() for v in vals.split(' | ')] if vals else []
                    self.ai_cache[name] = entry
            self.log_msg(f"Loaded AI cache: {len(self.ai_cache)} files")
        except Exception as e:
            self.log_msg(f"AI cache read error: {e}")

    def save_ai_cache_entry(self, filename: str, sugg: Dict[str,List[str]]):
        new_file = not AI_CACHE_CSV.exists()
        try:
            with AI_CACHE_CSV.open('a', newline='', encoding='utf-8') as f:
                w = csv.writer(f)
                if new_file:
                    w.writerow(['filename','date','provider','invoice','total','taxid','iban'])
                def join(key): return ' | '.join(sugg.get(key, []))
                w.writerow([filename, join('date'), join('provider'), join('invoice'), join('total'), join('taxid'), join('iban')])
        except Exception as e:
            self.log_msg(f"AI cache write error: {e}")

    def populate_ai_from_cache(self, filename: str):
        entry = self.ai_cache.get(filename)
        def current_value_for(key: str) -> str:
            if key == 'date':
                y, m, d = self.year_edit.text().strip(), self.month_edit.text().strip(), self.day_edit.text().strip()
                return f"{y}-{m}-{d}" if y or m or d else ""
            return {
                'provider': self.provider_edit.text().strip(),
                'invoice' : self.invoice_edit.text().strip(),
                'total'   : self.total_edit.text().strip(),
                'taxid'   : self.taxid_edit.text().strip(),
                'iban'    : self.iban_edit.text().strip(),
            }.get(key, "")
        for key, combo in self.sugg_fields.items():
            cur = current_value_for(key)
            candidates = entry.get(key, []) if entry else []
            combo.clear()
            # 1. AI candidates on top
            for v in candidates:
                combo.addItem(v)
            # 2. Current field value (if any) after candidates
            if cur:
                combo.addItem(cur)
            # 3. Finally the control options
            combo.addItem(LEAVE_UNCHANGED)
            combo.addItem(CLEAR_FIELD)
            # default select the first candidate if present
            if combo.count() > 0:
                combo.setCurrentIndex(0)

    # -------- AI (per file & batch) --------
    def run_ai(self, processor_id: str):
        if not _HAVE_DOCAI: self.log_msg("google-cloud-documentai not installed"); return
        if not self.files: return
        client = get_docai_client()
        if client is None: self.log_msg("Document AI client not available"); return
        path = self.files[self.idx]
        try:
            name = client.processor_path(PROJECT_ID, LOCATION, processor_id)
            with open(path, 'rb') as f:
                raw = docai.RawDocument(content=f.read(), mime_type='application/pdf')
            doc = client.process_document(request=docai.ProcessRequest(name=name, raw_document=raw)).document
            sugg = self.extract_entities(doc)
            self.ai_cache[path.name] = sugg
            self.save_ai_cache_entry(path.name, sugg)
            self.populate_ai_from_cache(path.name)
            self.log_msg(f"AI processed {path.name}, cached and suggestions updated")
        except Exception as e:
            self.log_msg(f"AI error: {e}")

    def batch_ai_cache(self):
        if not _HAVE_DOCAI: self.log_msg("google-cloud-documentai not installed"); return
        if not self.files: return
        client = get_docai_client()
        if client is None: self.log_msg("Document AI client not available"); return
        processed = 0
        for p in self.files:
            try:
                name = client.processor_path(PROJECT_ID, LOCATION, INVOICE_PROCESSOR_ID)
                with open(p, 'rb') as f:
                    raw = docai.RawDocument(content=f.read(), mime_type='application/pdf')
                doc = client.process_document(request=docai.ProcessRequest(name=name, raw_document=raw)).document
                sugg = self.extract_entities(doc)
                self.ai_cache[p.name] = sugg
                self.save_ai_cache_entry(p.name, sugg)
                processed += 1
                self.log_msg(f"[Batch] Cached {p.name} ({processed}/{len(self.files)})")
            except Exception as e:
                self.log_msg(f"[Batch] AI error on {p.name}: {e}")
        self.log_msg(f"Batch completed: {processed}/{len(self.files)} cached.")
        if self.files:
            self.populate_ai_from_cache(self.files[self.idx].name)

    def extract_entities(self, doc) -> Dict[str,List[str]]:
        suggestions: Dict[str,List[str]] = {}
        for ent in list(getattr(doc, 'entities', [])):
            et = (ent.type_ or "").lower()
            val = (ent.mention_text or "").strip()
            if not val: continue
            if et in ("invoice_id","invoice-number","invoice_number"):
                suggestions.setdefault("invoice", []).append(val)
            elif et in ("supplier_name","merchant","vendor","issuer","supplier"):
                suggestions.setdefault("provider", []).append(val)
            elif et in ("total_amount","grand_total","amount_total","invoice_total"):
                suggestions.setdefault("total", []).append(val)
            elif et in ("invoice_date","date","issue_date","purchase_date"):
                suggestions.setdefault("date", []).append(val)
            elif et in ("supplier_tax_id","vat_number","tax_id","vatid"):
                suggestions.setdefault("taxid", []).append(val)
        full_text = getattr(doc, "text", "") or ""
        for match in re.findall(r"[A-Z]{2}[0-9A-Z]{13,32}", full_text.replace(" ", "")):
            suggestions.setdefault("iban", []).append(match)
        return suggestions

    # -------- Apply AI selections --------
    def apply_suggestions(self):
        for key, combo in self.sugg_fields.items():
            if combo.count() == 0: continue
            sel = combo.currentText().strip()
            if sel == LEAVE_UNCHANGED or sel == "":
                continue
            if sel == CLEAR_FIELD:
                if key=="date": self.year_edit.clear(); self.month_edit.clear(); self.day_edit.clear()
                elif key=="provider": self.provider_edit.clear()
                elif key=="invoice":  self.invoice_edit.clear()
                elif key=="total":    self.total_edit.clear()
                elif key=="taxid":    self.taxid_edit.clear()
                elif key=="iban":     self.iban_edit.clear()
                continue
            if key == "date":
                parsed = parse_date_any(sel)
                if parsed:
                    y, mo, d = parsed
                    self.year_edit.setText(f"{y:04d}")
                    self.month_edit.setText(f"{mo:02d}")
                    self.day_edit.setText(f"{d:02d}")
                else:
                    self.log_msg(f"Date parse failed for '{sel}'")
            elif key=="provider": self.provider_edit.setText(sel)
            elif key=="invoice":  self.invoice_edit.setText(sel)
            elif key=="total":    self.total_edit.setText(sel)
            elif key=="taxid":    self.taxid_edit.setText(sel)
            elif key=="iban":     self.iban_edit.setText(sel)
        self.log_msg("Applied selected AI suggestions")

    # -------- Click-to-enrich on PDF --------
    def on_pdf_click(self, x: int, y: int):
        if not self.files: return
        path = self.files[self.idx]
        doc = self.try_open_doc(path)
        if doc is None: return
        try:
            pg = doc.load_page(self.page)
            pdf_x = x / self.zoom; pdf_y = y / self.zoom
            words = pg.get_text("words")
            if not words: 
                doc.close(); return
            best, best_dist2 = None, 1e18
            for w in words:
                x0,y0,x1,y1,word = w[0],w[1],w[2],w[3],w[4]
                cx,cy = (x0+x1)/2.0, (y0+y1)/2.0
                d2 = (cx-pdf_x)**2 + (cy-pdf_y)**2
                if d2 < best_dist2:
                    best_dist2 = d2; best = (word, x0,y0,x1,y1)
            doc.close()
            if not best: return
            text = (best[0] or "").strip()
            if not text: return
            menu = QtWidgets.QMenu(self)
            actions = {
                "Set Provider":        lambda: self.provider_edit.setText(text),
                "Set Invoice #":       lambda: self.invoice_edit.setText(text),
                "Set Total":           lambda: self.total_edit.setText(text),
                "Set Tax ID":          lambda: self.taxid_edit.setText(text),
                "Set IBAN":            lambda: self.iban_edit.setText(text),
                "Set Date (parse)":    lambda: self._apply_date_from_text(text),
            }
            for label, fn in actions.items():
                act = menu.addAction(label); act.triggered.connect(fn)
            menu.exec(QtGui.QCursor.pos())
        except Exception as e:
            self.log_msg(f"Click parse error: {e}")

    def _apply_date_from_text(self, txt: str):
        parsed = parse_date_any(txt)
        if parsed:
            y, mo, d = parsed
            self.year_edit.setText(f"{y:04d}")
            self.month_edit.setText(f"{mo:02d}")
            self.day_edit.setText(f"{d:02d}")
        else:
            self.log_msg(f"Date parse failed for '{txt}'")

# ---- Entrypoint ----
def main():
    app = QtWidgets.QApplication(sys.argv)
    start = Path(sys.argv[1]).expanduser() if len(sys.argv) > 1 else None
    w = PdfSorter(start)
    w.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
