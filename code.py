import os
import io
import re
import ssl
import json
import time
import uuid
import hashlib
import zipfile
import sqlite3
import threading
from pathlib import Path
from datetime import datetime
from ftplib import FTP, FTP_TLS
from xml.etree import ElementTree as ET

import requests
import paho.mqtt.client as mqtt
from flask import Flask, request, jsonify, render_template_string, send_file
from PIL import Image, ImageOps, ImageDraw

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "queue.db"
FLOWQ_OUTPUT_DIR = DATA_DIR / "generated_jobs"
FLOWQ_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

PREVIEW_OUTPUT_DIR = DATA_DIR / "generated_previews"
PREVIEW_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_PRINTER_MODEL = "Bambu P1S"
FIRST_AVAILABLE_LABEL = "First available"
APP_NAME = "LayerLoop"
APP_TAGLINE = "Open-source 3D printer automation"
APP_FILE_TAG = "layerloop"

MQTT_PORT = 8883
FTPS_PORT = 990
WEB_HOST = "0.0.0.0"
WEB_PORT = 5000

FTPS_TIMEOUT = 120
FTPS_BLOCKSIZE = 65536

QUEUE_POLL_SECONDS = 2
START_CONFIRM_TIMEOUT_SECONDS = 20
START_RETRY_LIMIT = 3

FLOWQ_ENABLED = True
FLOWQ_EJECT_GCODE_PATH = BASE_DIR / "eject_print.gcode"
FLOWQ_DEFAULT_COPIES = 2
FLOWQ_SAVE_GENERATED_LOCAL = True

AUTO_EJECT_NONE = "No - Manual Ready"
AUTO_EJECT_BETWEEN = "Yes - Eject Between Copies"
AUTO_EJECT_FINAL = "Yes - Final Eject Only"
AUTO_EJECT_ALWAYS = "Yes - Always Eject At End"

QUEUE_ITEM_TYPE_PRINT = "print"
QUEUE_ITEM_TYPE_FILAMENT_SWAP = "filament_swap"

FORCE_REGEN_PREVIEW = True

SPEED_SILENT = 1
SPEED_STANDARD = 2
SPEED_SPORT = 3
SPEED_LUDICROUS = 4

SPEED_OPTIONS = {
    SPEED_SILENT: "Silent",
    SPEED_STANDARD: "Standard",
    SPEED_SPORT: "Sport",
    SPEED_LUDICROUS: "Ludicrous",
}

SPEED_THROUGHPUT_MULTIPLIERS = {
    SPEED_SILENT: 0.50,
    SPEED_STANDARD: 1.00,
    SPEED_SPORT: 1.24,
    SPEED_LUDICROUS: 1.66,
}

MATERIAL_OPTIONS = [
    "PLA",
    "PLA+",
    "PETG",
    "ABS",
    "ASA",
    "TPU",
    "PC",
    "Nylon",
    "Carbon Fiber",
    "Generic",
]

BRAND_OPTIONS = [
    "Generic",
    "PolyLite",
    "Bambu",
    "eSun",
    "Prusament",
    "Overture",
    "Sunlu",
]

COLOR_OPTIONS = [
    "Black",
    "White",
    "Gray",
    "Blue",
    "Red",
    "Green",
    "Yellow",
    "Orange",
    "Purple",
    "Pink",
    "Transparent",
    "Custom",
]

COLOR_HEX_MAP = {
    "black": "#111111",
    "white": "#ffffff",
    "gray": "#9ca3af",
    "grey": "#9ca3af",
    "blue": "#3b82f6",
    "red": "#ef4444",
    "green": "#22c55e",
    "yellow": "#eab308",
    "orange": "#f97316",
    "purple": "#8b5cf6",
    "pink": "#ec4899",
    "transparent": "#d1d5db",
}

BRAND_VENDOR_MAP = {
    "bambu": "Bambu Lab",
}

MATERIAL_PROFILE_PRESETS = {
    "PLA": {"nozzle": 220, "bed": 55, "range_low": 190, "range_high": 240},
    "PLA+": {"nozzle": 225, "bed": 60, "range_low": 195, "range_high": 245},
    "PETG": {"nozzle": 255, "bed": 70, "range_low": 230, "range_high": 270},
    "ABS": {"nozzle": 260, "bed": 90, "range_low": 240, "range_high": 280},
    "ASA": {"nozzle": 260, "bed": 95, "range_low": 240, "range_high": 280},
    "TPU": {"nozzle": 230, "bed": 45, "range_low": 210, "range_high": 250},
    "PC": {"nozzle": 275, "bed": 110, "range_low": 255, "range_high": 295},
    "Nylon": {"nozzle": 270, "bed": 90, "range_low": 245, "range_high": 290},
    "Carbon Fiber": {"nozzle": 260, "bed": 80, "range_low": 240, "range_high": 280},
    "Generic": {},
}

AUTO_EJECTION_OPTIONS = [
    AUTO_EJECT_NONE,
    AUTO_EJECT_BETWEEN,
    AUTO_EJECT_FINAL,
    AUTO_EJECT_ALWAYS,
]

PREVIEW_OPTIONS = [
    "⬛", "🔷", "⬜", "🟧", "🟥", "🟩", "⚙️", "📦"
]

app = Flask(__name__)

HTML = r"""
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{{ app_name }}</title>
<style>
:root{
  --bg:#eef3f8;
  --panel:#ffffff;
  --panel2:#f8fafc;
  --border:#dfe7f1;
  --text:#141b24;
  --muted:#6b7280;
  --blue:#2a6df6;
  --blueSoft:#edf4ff;
  --green:#16a34a;
  --orange:#d97706;
  --red:#dc2626;
  --shadow:0 10px 30px rgba(20,27,36,.08);
}
*{box-sizing:border-box}
html,body{margin:0;padding:0;font-family:Inter,Arial,sans-serif;background:var(--bg);color:var(--text)}
.layout{display:grid;grid-template-columns:84px 1fr;min-height:100vh}
.sidebar{background:#fff;border-right:1px solid var(--border);padding:18px 12px;display:flex;flex-direction:column;align-items:center;gap:14px}
.logo{width:42px;height:42px;border-radius:14px;background:linear-gradient(135deg,#2465f2,#78a3ff);display:flex;align-items:center;justify-content:center;color:#fff;font-size:22px;font-weight:800;box-shadow:var(--shadow)}
.sidebtn{
  width:46px;height:46px;border-radius:14px;border:1px solid var(--border);background:#fff;
  display:flex;align-items:center;justify-content:center;color:#64748b;font-size:18px;cursor:pointer;
  transition:.15s ease;
}
.sidebtn:hover{transform:translateY(-1px);background:#f8fbff}
.sidebtn.active{background:var(--blueSoft);color:var(--blue);border-color:#cfe0ff}
.main{padding:24px 28px 32px}
.topbar{display:flex;justify-content:space-between;align-items:center;gap:16px;flex-wrap:wrap}
.title h1{margin:0;font-size:34px}
.title p{margin:8px 0 0;color:var(--muted)}
.top-actions{display:flex;gap:10px;flex-wrap:wrap}
.btn{border:none;border-radius:14px;padding:10px 15px;font-weight:700;font-size:14px;cursor:pointer;transition:.15s ease}
.btn:hover{transform:translateY(-1px)}
.btn-blue{background:var(--blue);color:#fff}
.btn-white{background:#fff;border:1px solid var(--border);color:var(--text)}
.btn-green{background:var(--green);color:#fff}
.btn-red{background:var(--red);color:#fff}
.btn-orange{background:var(--orange);color:#fff}
.tabs{display:flex;gap:20px;margin:22px 0 18px;border-bottom:1px solid var(--border)}
.tab{background:none;border:none;border-bottom:2px solid transparent;padding:0 0 12px;font-weight:800;color:var(--muted);cursor:pointer}
.tab.active{color:var(--text);border-bottom-color:var(--blue)}
.panel,.card{background:var(--panel);border:1px solid var(--border);border-radius:24px;box-shadow:var(--shadow)}
.panel{padding:18px}
.card{padding:18px}
.toolbar{display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:16px}
.search{display:flex;align-items:center;gap:10px;background:var(--panel2);border:1px solid var(--border);border-radius:14px;padding:0 12px;min-width:300px}
.search input{border:none;background:transparent;outline:none;padding:12px 0;width:100%;font-size:14px}
.queue-table-wrap{overflow:auto;border:1px solid var(--border);border-radius:18px;background:#fff}
.queue-table{width:100%;border-collapse:collapse;min-width:1500px}
.queue-table th{padding:14px 12px;text-align:left;font-size:12px;color:#667085;font-weight:800;background:#fbfcfe;border-bottom:1px solid var(--border);white-space:nowrap}
.queue-table td{padding:14px 12px;border-bottom:1px solid var(--border);vertical-align:middle;font-size:14px}
.queue-table tr:last-child td{border-bottom:none}
.preview{width:56px;height:56px;border-radius:14px;background:#f3f4f6;border:1px solid var(--border);display:flex;align-items:center;justify-content:center;font-size:18px;font-weight:700;overflow:hidden}
.preview img{width:100%;height:100%;object-fit:contain;display:block;background:#fff}
.name-cell .name{font-weight:800}
.name-cell .sub{font-size:12px;color:var(--muted);margin-top:4px;max-width:280px;word-break:break-word}
.material-dot{width:12px;height:12px;border-radius:999px;display:inline-block;margin-right:8px;vertical-align:middle;border:1px solid rgba(0,0,0,.08)}
.badge{display:inline-flex;align-items:center;gap:6px;padding:7px 10px;border-radius:999px;font-size:12px;font-weight:800;border:1px solid var(--border);background:#fff;white-space:nowrap}
.badge-blue{background:#ecf4ff;color:#1d5fe3;border-color:#cfe0ff}
.badge-green{background:#eafbf0;color:#137a38;border-color:#bfe8cb}
.badge-orange{background:#fff5e6;color:#a85c07;border-color:#ffd9a1}
.badge-red{background:#ffeded;color:#b42318;border-color:#ffcbcb}
.badge-gray{background:#f6f7f9;color:#586274;border-color:#e2e6eb}
.badge-purple{background:#f5edff;color:#6d28d9;border-color:#e9d5ff}
.row-actions{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.icon-btn{width:34px;height:34px;border-radius:10px;border:1px solid var(--border);background:#fff;cursor:pointer;display:flex;align-items:center;justify-content:center;color:#64748b;font-weight:800}
.icon-btn.wide{width:auto;padding:0 10px;font-size:12px}
.icon-btn[disabled]{opacity:.35;cursor:not-allowed}
.grid-2{display:grid;grid-template-columns:1.25fr .75fr;gap:18px;margin-top:18px}
.card h2{margin:0 0 14px;font-size:22px}
.card h3{margin:0 0 12px;font-size:16px}
.status-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px}
.stat{border:1px solid var(--border);border-radius:16px;padding:14px;background:var(--panel2)}
.stat .label{font-size:12px;color:var(--muted)}
.stat .value{font-size:22px;font-weight:800;margin-top:8px}
.progress-line{margin-top:8px;height:8px;border-radius:999px;background:#e5edf8;overflow:hidden}
.progress-fill{height:100%;width:0;background:var(--blue);transition:width .2s ease}
.eject-box{margin-top:14px;border:1px solid var(--border);border-radius:16px;background:var(--panel2);padding:14px}
.eject-line{margin-top:8px;color:var(--muted);font-size:14px}
.input, select.input{width:100%;border:1px solid var(--border);background:#fff;border-radius:12px;padding:11px 12px;font-size:14px}
.form-row{display:flex;gap:12px;flex-wrap:wrap}
.form-col{flex:1;min-width:220px}
.file-input{width:100%}
.checks{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px 14px;margin-top:12px}
.checks label{display:flex;align-items:center;gap:8px;font-size:14px;color:var(--text)}
.log{height:300px;overflow:auto;background:#0b1220;color:#d7dfef;border-radius:16px;padding:12px;font-family:Consolas,monospace;font-size:12px;white-space:pre-wrap}
.camera-shell{position:relative;width:100%;min-height:320px;border-radius:18px;border:1px solid var(--border);background:#050a14;overflow:hidden}
.camera{width:100%;min-height:320px;display:flex;align-items:center;justify-content:center;color:#9db0c8;padding:16px;text-align:center}
.camera img{width:100%;max-height:500px;object-fit:contain;display:block;background:#000}
.camera-overlay{position:absolute;left:12px;bottom:12px;display:flex;gap:8px;z-index:5}
.camera-toggle-btn{background:rgba(13,23,40,.92);border:1px solid #344768;color:#fff;border-radius:12px;padding:10px 14px;font-size:14px;font-weight:700;cursor:pointer}
.camera-status-pill{position:absolute;right:12px;bottom:12px;z-index:5;background:rgba(13,23,40,.92);border:1px solid #344768;color:#c6d4ea;border-radius:12px;padding:10px 14px;font-size:13px;font-weight:700}
.hidden{display:none}
.empty{padding:40px 20px;text-align:center;color:var(--muted)}
.small{font-size:12px;color:var(--muted)}
.timer-cell .big{font-weight:800}
.timer-cell .sub{font-size:12px;color:var(--muted);margin-top:4px}
.swap-row td{background:#fcf8ff}
.swap-note{font-size:12px;color:#7c3aed;margin-top:4px}
.modal-backdrop{
  position:fixed;inset:0;background:rgba(5,10,20,.56);display:none;
  align-items:center;justify-content:center;z-index:1000;padding:20px;overflow-y:auto;
}
.modal-backdrop.show{display:flex}
.modal{
  width:min(520px,100%);background:#fff;border-radius:28px;border:1px solid var(--border);
  box-shadow:0 28px 80px rgba(20,27,36,.25);padding:26px;max-height:calc(100vh - 40px);overflow-y:auto;
}
.modal.settings-modal{width:min(860px,100%)}
.modal h2{margin:0 0 10px;font-size:28px}
.modal p{margin:0 0 12px;color:var(--muted);line-height:1.5}
.modal-icon{
  width:76px;height:76px;border-radius:22px;background:#f5edff;color:#6d28d9;
  display:flex;align-items:center;justify-content:center;font-size:36px;font-weight:800;margin-bottom:16px;
}
.modal-actions{display:flex;gap:12px;flex-wrap:wrap;margin-top:18px}
.modal-task{
  margin-top:14px;padding:14px;border-radius:16px;background:#f8fafc;border:1px solid var(--border);
  font-size:14px;
}
.settings-grid{
  display:grid;
  grid-template-columns:1fr 1fr;
  gap:12px;
  margin-top:14px;
}
.settings-card{
  border:1px solid var(--border);
  border-radius:16px;
  padding:14px;
  background:#f8fafc;
}
.settings-card-wide{grid-column:1 / -1}
.settings-card .title{
  font-size:14px;
  font-weight:800;
  margin:0 0 6px;
}
.settings-card .desc{
  font-size:13px;
  color:var(--muted);
  line-height:1.45;
  margin:0 0 12px;
}
.printer-summary-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:10px;margin:0 0 14px}
.printer-summary-card{border:1px solid var(--border);border-radius:16px;padding:12px;background:#f8fafc}
.printer-summary-card .title{font-size:14px;font-weight:800}
.printer-summary-card .sub{font-size:12px;color:var(--muted);margin-top:4px}
.printer-summary-card .meta{font-size:12px;color:var(--muted);margin-top:8px}
.printer-manager-list{display:flex;flex-direction:column;gap:10px;margin-top:12px}
.printer-manager-item{border:1px solid var(--border);border-radius:14px;padding:12px;background:#fff}
.printer-manager-item .name{font-weight:800}
.printer-manager-item .sub{font-size:12px;color:var(--muted);margin-top:4px;word-break:break-word}
.printer-manager-item .status{margin-top:8px;font-size:12px;font-weight:700}
.settings-stack{display:flex;flex-direction:column;gap:12px}
.divider{height:1px;background:var(--border);margin:12px 0}
.toast-wrap{
  position:fixed;
  top:20px;
  left:20px;
  z-index:3000;
  display:flex;
  flex-direction:column;
  gap:12px;
  pointer-events:none;
  max-width:min(440px, calc(100vw - 40px));
}
.toast{
  pointer-events:auto;
  display:flex;
  align-items:flex-start;
  gap:12px;
  background:#fff;
  border:1px solid var(--border);
  border-left:5px solid var(--blue);
  border-radius:18px;
  padding:14px 16px;
  box-shadow:0 18px 50px rgba(20,27,36,.16);
  animation:toastIn .18s ease;
}
.toast.success{border-left-color:var(--green)}
.toast.error{border-left-color:var(--red)}
.toast.info{border-left-color:var(--blue)}
.toast.warning{border-left-color:var(--orange)}
.toast-icon{
  width:34px;height:34px;border-radius:12px;display:flex;align-items:center;justify-content:center;
  font-size:16px;font-weight:800;flex:0 0 34px;background:#f8fafc;border:1px solid var(--border);
}
.toast-text{min-width:0}
.toast-title{font-weight:800;font-size:14px;line-height:1.25}
.toast-message{font-size:13px;color:var(--muted);margin-top:4px;line-height:1.4;word-break:break-word}
.toast-close{
  margin-left:auto;border:none;background:transparent;cursor:pointer;font-size:18px;
  color:#6b7280;line-height:1;padding:0 0 0 8px;
}
.toast.removing{animation:toastOut .18s ease forwards}
@keyframes toastIn{
  from{opacity:0;transform:translateY(-8px) scale(.98)}
  to{opacity:1;transform:translateY(0) scale(1)}
}
@keyframes toastOut{
  from{opacity:1;transform:translateY(0) scale(1)}
  to{opacity:0;transform:translateY(-8px) scale(.98)}
}
@media (max-width:1200px){.grid-2{grid-template-columns:1fr}}
@media (max-width:900px){
  .modal.settings-modal{width:min(680px,100%)}
  .settings-grid{grid-template-columns:1fr}
  .settings-card-wide{grid-column:auto}
}
@media (max-width:760px){
  .layout{grid-template-columns:1fr}
  .sidebar{display:none}
  .main{padding:18px}
  .status-grid{grid-template-columns:1fr 1fr}
  .settings-grid{grid-template-columns:1fr}
  .toast-wrap{top:14px;left:14px;right:14px;max-width:none}
}
</style>
</head>
<body>
<div id="toastWrap" class="toast-wrap"></div>

<div class="layout">
  <aside class="sidebar">
    <div class="logo">∞</div>
    <button class="sidebtn active" id="navQueueBtn" onclick="openSection('queueTab', 'navQueueBtn')">≡</button>
    <button class="sidebtn" id="navBuilderBtn" onclick="openSection('builderTab', 'navBuilderBtn')">⌂</button>
    <button class="sidebtn" id="navMachineBtn" onclick="openSection('machineTab', 'navMachineBtn')">◫</button>
    <button class="sidebtn" id="navSettingsBtn" onclick="openSettingsModal()">⚙</button>
  </aside>

  <main class="main">
    <div class="topbar">
      <div class="title">
        <h1>Queue</h1>
        <p id="topSubtitle">{{ app_tagline }}</p>
      </div>
      <div class="top-actions">
        <button class="btn btn-white" onclick="manualRefreshStatus()">Refresh status</button>
        <button class="btn btn-white" onclick="manualReloadQueue()">Reload queue</button>
        <button class="btn btn-white" onclick="reloadCamera(true)">Reload camera</button>
        <button class="btn btn-blue" onclick="openSpeedModal()">Set speed for all prints</button>
        <button class="btn btn-orange" onclick="addSwapAtEnd()">Add filament swap at end</button>
        <button class="btn btn-green" id="autorunBtn" onclick="toggleAutorun()">Autorun On</button>
      </div>
    </div>

    <div class="tabs">
      <button class="tab active" id="tabQueueBtn" onclick="openSection('queueTab', 'navQueueBtn', 'tabQueueBtn')">Queue</button>
      <button class="tab" id="tabBuilderBtn" onclick="openSection('builderTab', 'navBuilderBtn', 'tabBuilderBtn')">Print Files</button>
      <button class="tab" id="tabMachineBtn" onclick="openSection('machineTab', 'navMachineBtn', 'tabMachineBtn')">Machine</button>
    </div>

    <section id="queueTab">
      <div class="panel">
        <div class="toolbar">
          <div class="search">
            <span>⌕</span>
            <input id="queueSearch" placeholder="Search..." oninput="renderQueue()">
          </div>
          <div class="row-actions">
            <button class="btn btn-white" onclick="manualReloadQueue()">Refresh Queue</button>
          </div>
        </div>
        <div class="queue-table-wrap">
          <table class="queue-table">
            <thead>
              <tr>
                <th>Preview</th>
                <th>Name</th>
                <th>Printer</th>
                <th>Material</th>
                <th>Color</th>
                <th>Timer</th>
                <th>Repetitions</th>
                <th>Ejection</th>
                <th>Status</th>
                <th style="width:240px"></th>
              </tr>
            </thead>
            <tbody id="queueBody">
              <tr><td colspan="10" class="empty">Loading queue...</td></tr>
            </tbody>
          </table>
        </div>
      </div>
    </section>

    <section id="builderTab" class="hidden">
      <div class="grid-2">
        <div class="card">
          <h2>Print builder</h2>
          <form id="buildPrintForm">
            <label>Choose source .gcode.3mf</label>
            <input class="input file-input" type="file" id="sourceFileInput" name="file" accept=".3mf,.gcode.3mf" required>

            <div class="form-row" style="margin-top:12px">
              <div class="form-col">
                <label>How many times to print it</label>
                <input class="input" id="copiesInput" type="number" name="copies" min="1" max="50" value="{{ flowq_default_copies }}" required>
              </div>
              <div class="form-col">
                <label>Override minutes per copy</label>
                <input class="input" id="minutesOverride" type="number" name="minutes_per_copy" min="0" max="5000" value="0">
                <div class="small" id="detectedTimeInfo">Auto detect waits for a file.</div>
              </div>
            </div>

            <div class="form-row" style="margin-top:12px">
              <div class="form-col">
                <label>Material</label>
                <select class="input" name="material">
                  {% for item in material_options %}
                  <option value="{{ item }}">{{ item }}</option>
                  {% endfor %}
                </select>
              </div>
              <div class="form-col">
                <label>Brand</label>
                <select class="input" name="brand">
                  {% for item in brand_options %}
                  <option value="{{ item }}">{{ item }}</option>
                  {% endfor %}
                </select>
              </div>
              <div class="form-col">
                <label>Color</label>
                <select class="input" name="color">
                  {% for item in color_options %}
                  <option value="{{ item }}">{{ item }}</option>
                  {% endfor %}
                </select>
              </div>
              <div class="form-col">
                <label>Speed</label>
                <select class="input" name="speed_level">
                  <option value="1">Silent</option>
                  <option value="2" selected>Standard</option>
                  <option value="3">Sport</option>
                  <option value="4">Ludicrous</option>
                </select>
              </div>
            </div>

            <div class="form-row" style="margin-top:12px">
              <div class="form-col">
                <label>Automatic print ejection</label>
                <select class="input" name="auto_ejection">
                  {% for item in auto_ejection_options %}
                  <option value="{{ item }}">{{ item }}</option>
                  {% endfor %}
                </select>
              </div>
              <div class="form-col">
                <label>Fallback icon if no 3MF preview exists</label>
                <select class="input" name="preview_emoji">
                  {% for item in preview_options %}
                  <option value="{{ item }}">{{ item }}</option>
                  {% endfor %}
                </select>
              </div>
              <div class="form-col">
                <label>Print on</label>
                <select class="input" name="target_printer_id" id="targetPrinterSelect">
                  {% for item in printer_options %}
                  <option value="{{ item.value }}">{{ item.label }}</option>
                  {% endfor %}
                </select>
              </div>
            </div>

            <div class="checks">
              <label><input type="checkbox" name="timelapse" checked> Timelapse</label>
              <label><input type="checkbox" name="bed_levelling" checked> Bed leveling</label>
              <label><input type="checkbox" name="flow_cali" checked> Flow calibration</label>
              <label><input type="checkbox" name="vibration_cali" checked> Vibration calibration</label>
              <label><input type="checkbox" name="layer_inspect" checked> Layer inspect</label>
              <label><input type="checkbox" name="use_ams"> Use AMS</label>
            </div>

            <div class="form-row" style="margin-top:16px">
              <button class="btn btn-blue" type="submit">Add to Queue</button>
            </div>
          </form>
        </div>

        <div class="card">
          <h2>Queue tips</h2>
          <div class="eject-box">
            <div class="eject-line">Generated files are saved in: {{ flowq_output_dir }}</div>
            <div class="eject-line">Previews are extracted from the .3mf file when possible.</div>
            <div class="eject-line">Use “Add filament swap at end” or the row button to force a material change stop.</div>
            <div class="eject-line">When a filament swap stop is reached, queue autorun pauses until you press Continue.</div>
            <div class="eject-line">If ejection is enabled, make sure eject_print.gcode exists next to this script.</div>
          </div>
        </div>
      </div>
    </section>

    <section id="machineTab" class="hidden">
      <div class="grid-2">
        <div>
          <div class="card">
            <h2>Machine Status</h2>
            <div class="form-row" style="margin-bottom:14px">
              <div class="form-col">
                <label for="machinePrinterSelect">Selected printer</label>
                <select id="machinePrinterSelect" class="input" onchange="changeSelectedMachinePrinter(this.value)">
                  {% for item in printer_options if item.value %}
                  <option value="{{ item.value }}" {% if item.value == initial_selected_printer_id %}selected{% endif %}>{{ item.label }}</option>
                  {% endfor %}
                </select>
              </div>
            </div>

            <div id="printerSummaryGrid" class="printer-summary-grid"></div>

            <div class="status-grid">
              <div class="stat"><div class="label">State</div><div class="value" id="state">Disconnected</div></div>
              <div class="stat"><div class="label">Progress</div><div class="value"><span id="percent">0</span>%<div class="progress-line"><div class="progress-fill" id="percentBar"></div></div></div></div>
              <div class="stat"><div class="label">Layer</div><div class="value" id="layer">0 / 0</div></div>
              <div class="stat"><div class="label">Nozzle</div><div class="value" id="nozzle">0 / 0 °C</div></div>
              <div class="stat"><div class="label">Bed</div><div class="value" id="bed">0 / 0 °C</div></div>
              <div class="stat"><div class="label">Time left</div><div class="value" id="timeleft">-</div></div>
            </div>

            <div style="margin-top:16px">
              <h3>Print controls</h3>
              <div class="form-row">
                <button class="btn btn-white" onclick="pauseSelectedPrinter()">Pause</button>
                <button class="btn btn-green" onclick="resumeSelectedPrinter()">Resume</button>
                <button class="btn btn-red" onclick="stopSelectedPrinter()">Stop</button>
              </div>
            </div>
          </div>

          <div class="card" style="margin-top:18px">
            <h2>Log</h2>
            <div class="log" id="log"></div>
          </div>
        </div>

        <div>
          <div class="card">
            <h2>Camera</h2>
            <div class="camera-shell">
              <div id="cameraBox" class="camera">No camera bridge configured.</div>
              <div class="camera-overlay">
                <button id="cameraToggleBtn" class="camera-toggle-btn" onclick="toggleCamera()">Camera Off</button>
              </div>
              <div id="cameraStateLabel" class="camera-status-pill">Camera On</div>
            </div>
          </div>
        </div>
      </div>
    </section>
  </main>
</div>

<div id="swapModalBackdrop" class="modal-backdrop">
  <div class="modal settings-modal">
    <div class="modal-icon">🧵</div>
    <h2>Tasks paused</h2>
    <p>Please swap the filament and click continue.</p>
    <div id="swapModalTask" class="modal-task">Waiting for filament swap task...</div>
    <div class="modal-actions">
      <button class="btn btn-green" onclick="continueFilamentSwap()">Continue</button>
      <button class="btn btn-white" onclick="reloadQueue();refreshStatus();">Refresh</button>
    </div>
  </div>
</div>

<div id="speedModalBackdrop" class="modal-backdrop">
  <div class="modal">
    <div class="modal-icon">⚡</div>
    <h2>Set speed for all prints</h2>
    <p>Choose the speed that should be used for every print in the queue.</p>
    <div class="modal-task">
      <label for="speedLevelSelect" style="display:block;margin-bottom:8px;font-weight:700;color:#141b24;">Print speed</label>
      <select id="speedLevelSelect" class="input">
        <option value="1">Silent</option>
        <option value="2" selected>Standard</option>
        <option value="3">Sport</option>
        <option value="4">Ludicrous</option>
      </select>
    </div>
    <div class="modal-actions">
      <button class="btn btn-green" onclick="saveSpeedForAll()">Save</button>
      <button class="btn btn-white" onclick="closeSpeedModal()">Cancel</button>
    </div>
  </div>
</div>

<div id="settingsModalBackdrop" class="modal-backdrop">
  <div class="modal">
    <div class="modal-icon">⚙</div>
    <h2>Quick settings</h2>
    <p>Fast controls for the printer and queue.</p>

    <div class="settings-grid">
      <div class="settings-card">
        <div class="title">Queue</div>
        <div class="desc">Refresh the queue list or toggle autorun.</div>
        <div class="form-row">
          <button class="btn btn-white" onclick="manualReloadQueue()">Reload queue</button>
          <button class="btn btn-green" onclick="toggleAutorun()">Autorun</button>
        </div>
      </div>

      <div class="settings-card">
        <div class="title">Printer</div>
        <div class="desc">Refresh machine state or open machine page.</div>
        <div class="form-row">
          <button class="btn btn-white" onclick="manualRefreshStatus()">Refresh status</button>
          <button class="btn btn-blue" onclick="openSection('machineTab', 'navMachineBtn', 'tabMachineBtn'); closeSettingsModal();">Machine</button>
        </div>
      </div>

      <div class="settings-card">
        <div class="title">Camera</div>
        <div class="desc">Reload or toggle the live camera view.</div>
        <div class="form-row">
          <button class="btn btn-white" onclick="reloadCamera(true)">Reload camera</button>
          <button class="btn btn-blue" onclick="toggleCamera()">Toggle camera</button>
        </div>
      </div>

      <div class="settings-card settings-card-wide">
        <div class="title">Queue speed</div>
        <div class="desc">Change speed for all queued prints.</div>
        <div class="form-row">
          <button class="btn btn-orange" onclick="closeSettingsModal(); openSpeedModal();">Set all speeds</button>
        </div>
      </div>

      <div class="settings-card">
        <div class="title">Printers</div>
        <div class="desc">Add P1S printers here and choose which machine the controls page should use.</div>
        <div class="settings-stack">
          <div id="printerManagerList" class="printer-manager-list"></div>
          <div class="divider"></div>
          <div class="form-row">
            <div class="form-col">
              <label for="printerNameInput">Printer name</label>
              <input id="printerNameInput" class="input" placeholder="Printer 2">
            </div>
            <div class="form-col">
              <label for="printerIpInput">IP address</label>
              <input id="printerIpInput" class="input" placeholder="192.168.178.221">
            </div>
          </div>
          <div class="form-row">
            <div class="form-col">
              <label for="printerAccessCodeInput">Access code</label>
              <input id="printerAccessCodeInput" class="input" placeholder="Printer access code">
            </div>
            <div class="form-col">
              <label for="printerSerialInput">Serial</label>
              <input id="printerSerialInput" class="input" placeholder="01P00...">
            </div>
          </div>
          <div class="form-row">
            <div class="form-col">
              <label for="printerCameraUrlInput">Camera URL (optional)</label>
              <input id="printerCameraUrlInput" class="input" placeholder="http://127.0.0.1:1984/api/stream.mjpeg?src=p1s-2">
            </div>
          </div>
          <div class="form-row">
            <button class="btn btn-blue" onclick="addPrinter()">Add P1S Printer</button>
          </div>
        </div>
      </div>
    </div>

    <div class="modal-actions">
      <button class="btn btn-white" onclick="closeSettingsModal()">Close</button>
    </div>
  </div>
</div>

<script>
const CAMERA_STORAGE_KEY = 'bambu_camera_enabled';
let queueItems = [];
let queueAutorunEnabled = true;
let currentQueueId = null;
let printerStatus = {};
let printerStates = [];
let printerConfigs = [];
let printerOptions = [];
let selectedMachinePrinterId = {{ initial_selected_printer_id|tojson }};
let activeFilamentSwapId = null;
const APP_NAME = {{ app_name|tojson }};
const APP_TAGLINE = {{ app_tagline|tojson }};

function qs(id){ return document.getElementById(id); }

function escapeHtml(s){
  return String(s ?? '')
    .replaceAll('&','&amp;')
    .replaceAll('<','&lt;')
    .replaceAll('>','&gt;')
    .replaceAll('"','&quot;')
    .replaceAll("'","&#39;");
}

function toastIcon(type){
  if (type === 'success') return '✓';
  if (type === 'error') return '!';
  if (type === 'warning') return '⚠';
  return 'i';
}

function showToast(title, message='', type='info', duration=3200){
  const wrap = qs('toastWrap');
  if (!wrap) return;

  const el = document.createElement('div');
  el.className = `toast ${type}`;
  el.innerHTML = `
    <div class="toast-icon">${toastIcon(type)}</div>
    <div class="toast-text">
      <div class="toast-title">${escapeHtml(title || 'Notice')}</div>
      <div class="toast-message">${escapeHtml(message || '')}</div>
    </div>
    <button class="toast-close" aria-label="Close">×</button>
  `;

  const remove = () => {
    if (!el.parentNode) return;
    el.classList.add('removing');
    setTimeout(() => el.remove(), 180);
  };

  el.querySelector('.toast-close').onclick = remove;
  wrap.prepend(el);

  if (duration > 0) {
    setTimeout(remove, duration);
  }
}

function setActiveSidebarButton(sidebarId) {
  document.querySelectorAll('.sidebtn').forEach(btn => btn.classList.remove('active'));
  const btn = qs(sidebarId);
  if (btn) btn.classList.add('active');
}

function setActiveTopTab(tabId) {
  document.querySelectorAll('.tab').forEach(tab => tab.classList.remove('active'));
  const tab = qs(tabId);
  if (tab) tab.classList.add('active');
}

function openSection(sectionId, sidebarId, tabId=null){
  for(const sec of ['queueTab','builderTab','machineTab']) qs(sec).classList.add('hidden');
  qs(sectionId).classList.remove('hidden');

  setActiveSidebarButton(sidebarId);

  if (sectionId === 'queueTab') setActiveTopTab('tabQueueBtn');
  if (sectionId === 'builderTab') setActiveTopTab('tabBuilderBtn');
  if (sectionId === 'machineTab') setActiveTopTab('tabMachineBtn');

  if (tabId) setActiveTopTab(tabId);

  if (sectionId === 'queueTab') showToast('Queue opened', 'Showing all queued jobs.', 'info', 1800);
  if (sectionId === 'builderTab') showToast('Print Files opened', 'Ready to add a new file to the queue.', 'info', 1800);
  if (sectionId === 'machineTab') showToast('Machine opened', 'Showing live printer status.', 'info', 1800);
}

function openSettingsModal() {
  setActiveSidebarButton('navSettingsBtn');
  qs('settingsModalBackdrop').classList.add('show');
}

function closeSettingsModal() {
  qs('settingsModalBackdrop').classList.remove('show');

  if (!qs('queueTab').classList.contains('hidden')) setActiveSidebarButton('navQueueBtn');
  else if (!qs('builderTab').classList.contains('hidden')) setActiveSidebarButton('navBuilderBtn');
  else if (!qs('machineTab').classList.contains('hidden')) setActiveSidebarButton('navMachineBtn');
  else setActiveSidebarButton('navQueueBtn');
}

function openSpeedModal() {
  qs('speedModalBackdrop').classList.add('show');
}

function closeSpeedModal() {
  qs('speedModalBackdrop').classList.remove('show');
}

async function saveSpeedForAll() {
  const speedLevel = Number(qs('speedLevelSelect').value || 2);
  const result = await postJson('/api/queue/set_speed_all', { speed_level: speedLevel }, {
    toastTitle: 'Queue speed updated'
  });
  if (result && result.ok) {
    closeSpeedModal();
    await reloadQueue();
    await refreshStatus();
  }
}

function selectedPrinterRequest() {
  return { printer_id: selectedMachinePrinterId || '' };
}

function renderBuilderPrinterOptions() {
  const select = qs('targetPrinterSelect');
  if (!select) return;
  const current = select.value;
  const options = printerOptions.length ? printerOptions : [{ value: '', label: 'First available' }];
  select.innerHTML = options.map(item => `<option value="${escapeHtml(item.value)}">${escapeHtml(item.label)}</option>`).join('');
  select.value = options.some(item => item.value === current) ? current : (options[0]?.value || '');
}

function renderMachinePrinterSelect() {
  const select = qs('machinePrinterSelect');
  if (!select) return;
  const options = printerConfigs.map(item => ({ value: item.id, label: item.name }));
  select.innerHTML = options.map(item => `<option value="${escapeHtml(item.value)}">${escapeHtml(item.label)}</option>`).join('');
  if (!selectedMachinePrinterId && options.length) {
    selectedMachinePrinterId = options[0].value;
  }
  if (options.some(item => item.value === selectedMachinePrinterId)) {
    select.value = selectedMachinePrinterId;
  }
}

function renderPrinterSummaryGrid() {
  const el = qs('printerSummaryGrid');
  if (!el) return;
  if (!printerStates.length) {
    el.innerHTML = '';
    return;
  }

  el.innerHTML = printerStates.map(item => `
    <div class="printer-summary-card">
      <div class="title">${escapeHtml(item.name || 'Printer')}</div>
      <div class="sub">${escapeHtml(item.gcode_state || 'Disconnected')}</div>
      <div class="meta">${escapeHtml(item.connected ? 'Connected' : 'Disconnected')} • ${fmtSeconds(item.remaining_time_seconds || 0)} left</div>
    </div>
  `).join('');
}

function renderPrinterManagerList() {
  const el = qs('printerManagerList');
  if (!el) return;
  if (!printerConfigs.length) {
    el.innerHTML = '<div class="printer-manager-item"><div class="name">No printers yet</div><div class="sub">Add your first P1S below.</div></div>';
    return;
  }

  el.innerHTML = printerConfigs.map(item => `
    <div class="printer-manager-item">
      <div class="name">${escapeHtml(item.name || 'Printer')}</div>
      <div class="sub">${escapeHtml(item.ip || '-')} • ${escapeHtml(item.serial || '-')}</div>
      <div class="status">${escapeHtml(item.connected ? 'Connected' : 'Disconnected')} • ${escapeHtml(item.gcode_state || 'Unknown')}</div>
    </div>
  `).join('');
}

async function loadPrinters() {
  try {
    const r = await fetch('/api/printers');
    const j = await r.json();
    printerConfigs = j.items || [];
    printerOptions = j.printer_options || [];
    selectedMachinePrinterId = j.selected_printer_id || selectedMachinePrinterId;
    renderBuilderPrinterOptions();
    renderMachinePrinterSelect();
    renderPrinterManagerList();
  } catch (e) {
    addLog('Printer list failed: ' + e);
  }
}

async function changeSelectedMachinePrinter(printerId) {
  if (!printerId) return;
  const result = await postJson('/api/printers/select', { printer_id: printerId }, {
    toastTitle: 'Machine printer selected'
  });
  if (result && result.ok) {
    selectedMachinePrinterId = printerId;
    await loadPrinters();
    await refreshStatus();
  }
}

async function addPrinter() {
  const payload = {
    name: qs('printerNameInput').value.trim(),
    ip: qs('printerIpInput').value.trim(),
    access_code: qs('printerAccessCodeInput').value.trim(),
    serial: qs('printerSerialInput').value.trim(),
    camera_url: qs('printerCameraUrlInput').value.trim()
  };
  const result = await postJson('/api/printers/add', payload, {
    toastTitle: 'Printer added'
  });
  if (result && result.ok) {
    ['printerNameInput', 'printerIpInput', 'printerAccessCodeInput', 'printerSerialInput', 'printerCameraUrlInput']
      .forEach(id => { if (qs(id)) qs(id).value = ''; });
    await loadPrinters();
    await refreshStatus();
    await reloadQueue();
  }
}

async function pauseSelectedPrinter() {
  await postJson('/api/pause', selectedPrinterRequest(), { toastTitle: 'Printer paused' });
}

async function resumeSelectedPrinter() {
  await postJson('/api/resume', selectedPrinterRequest(), { toastTitle: 'Printer resumed' });
}

async function stopSelectedPrinter() {
  await postJson('/api/stop', selectedPrinterRequest(), { toastTitle: 'Print stopped' });
}

function withCacheBust(url) {
  if (!url) return url;
  return url + (url.includes('?') ? '&t=' : '?t=') + Date.now();
}

function isCameraEnabled() {
  const saved = localStorage.getItem(CAMERA_STORAGE_KEY);
  if (saved === null) return true;
  return saved === 'true';
}

function setCameraEnabled(enabled) {
  localStorage.setItem(CAMERA_STORAGE_KEY, enabled ? 'true' : 'false');
}

function updateCameraUiState() {
  const enabled = isCameraEnabled();
  qs('cameraToggleBtn').textContent = enabled ? 'Camera Off' : 'Camera On';
  qs('cameraStateLabel').textContent = enabled ? 'Camera On' : 'Camera Off';
}

function renderCamera() {
  const box = qs('cameraBox');
  const url = printerStatus.camera_url || '';
  const enabled = !!url;
  const localEnabled = isCameraEnabled();

  if (!enabled || !url) {
    box.textContent = 'No camera bridge configured.';
    updateCameraUiState();
    return;
  }

  if (!localEnabled) {
    box.innerHTML = '<div style="color:#9db0c8;font-weight:700">Camera is turned off</div>';
    updateCameraUiState();
    return;
  }

  box.innerHTML = `<img id="cameraImg" src="${withCacheBust(url)}" alt="Camera">`;
  updateCameraUiState();
}

function reloadCamera(showPopup=false) {
  const url = printerStatus.camera_url || '';
  const img = qs('cameraImg');
  if (img && url && isCameraEnabled()) {
    img.src = withCacheBust(url);
  } else if (isCameraEnabled()) {
    renderCamera();
  }

  if (showPopup) {
    showToast('Camera reloaded', 'The camera view was refreshed.', 'success');
  }
}

function toggleCamera() {
  const next = !isCameraEnabled();
  setCameraEnabled(next);
  renderCamera();
  showToast(
    next ? 'Camera turned on' : 'Camera turned off',
    next ? 'Live camera view is enabled.' : 'Live camera view is disabled.',
    'info'
  );
}

function addLog(t) {
  const el = qs('log');
  el.textContent += `[${new Date().toLocaleTimeString()}] ${t}\n`;
  el.scrollTop = el.scrollHeight;
}

function fmtSeconds(totalSeconds) {
  totalSeconds = Number(totalSeconds || 0);
  if (!Number.isFinite(totalSeconds) || totalSeconds < 0) totalSeconds = 0;
  const h = Math.floor(totalSeconds / 3600);
  const m = Math.floor((totalSeconds % 3600) / 60);
  const s = Math.floor(totalSeconds % 60);
  if (h > 0) return `${h}h ${m}m ${s}s`;
  if (m > 0) return `${m}m ${s}s`;
  return `${s}s`;
}

async function postJson(url, data={}, options={}) {
  const {
    toast = true,
    toastTitle = '',
    successDuration = 3200,
    errorDuration = 4200
  } = options;

  try {
    const r = await fetch(url, {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify(data)
    });

    const j = await r.json();
    addLog(j.message || JSON.stringify(j));

    if (toast) {
      const title = toastTitle || (j.ok ? 'Action completed' : 'Action failed');
      showToast(title, j.message || '', j.ok ? 'success' : 'error', j.ok ? successDuration : errorDuration);
    }

    refreshStatus();
    reloadQueue();
    return j;
  } catch (e) {
    const msg = 'Request failed: ' + e;
    addLog(msg);
    if (toast) {
      showToast(toastTitle || 'Request failed', String(e), 'error', errorDuration);
    }
    return { ok:false, message:String(e) };
  }
}

function statusBadge(item){
  const status = (item.status || '').toLowerCase();
  const type = (item.item_type || 'print').toLowerCase();
  if (type === 'filament_swap') {
    if (status === 'waiting' || item.is_current) return `<span class="badge badge-purple">Swap Waiting</span>`;
    if (status === 'done') return `<span class="badge badge-gray">Swap Done</span>`;
    if (status === 'error') return `<span class="badge badge-red">Error</span>`;
    return `<span class="badge badge-purple">Filament Swap</span>`;
  }
  if (item.is_current || status === 'printing') return `<span class="badge badge-green">Printing</span>`;
  if (status === 'queued') return `<span class="badge badge-blue">Queued</span>`;
  if (status === 'starting') return `<span class="badge badge-orange">Starting</span>`;
  if (status === 'done') return `<span class="badge badge-gray">Done</span>`;
  if (status === 'error') return `<span class="badge badge-red">Error</span>`;
  return `<span class="badge badge-gray">${item.status || '-'}</span>`;
}

function colorDotStyle(color) {
  const c = String(color || '').toLowerCase();
  if (c.includes('blue')) return '#3b82f6';
  if (c.includes('white')) return '#ffffff';
  if (c.includes('gray') || c.includes('grey')) return '#9ca3af';
  if (c.includes('red')) return '#ef4444';
  if (c.includes('orange')) return '#f97316';
  if (c.includes('green')) return '#22c55e';
  if (c.includes('yellow')) return '#eab308';
  if (c.includes('purple')) return '#8b5cf6';
  if (c.includes('pink')) return '#ec4899';
  if (c.includes('transparent')) return '#d1d5db';
  return '#111111';
}

function getQueueTimerDisplay(item) {
  if ((item.item_type || 'print') === 'filament_swap') {
    return {
      main: 'Manual',
      sub: 'Waits for continue'
    };
  }

  if (item.is_current && item.live_remaining_time_seconds != null) {
    return {
      main: fmtSeconds(item.live_remaining_time_seconds),
      sub: item.current_printer_name ? `Live time left on ${item.current_printer_name}` : 'Live printer time left'
    };
  }

  if (item.status === 'done' && item.actual_total_seconds > 0) {
    return {
      main: fmtSeconds(item.actual_total_seconds),
      sub: 'Actual total'
    };
  }

  if (item.estimated_total_seconds > 0) {
    return {
      main: fmtSeconds(item.estimated_total_seconds),
      sub: 'Estimated total'
    };
  }

  return {
    main: escapeHtml(item.duration || '-'),
    sub: 'Estimate unavailable'
  };
}

function getPreviewHtml(item) {
  if ((item.item_type || 'print') === 'filament_swap') {
    return '🧵';
  }
  if (item.preview_url) {
    return `<img src="${item.preview_url}" alt="preview">`;
  }
  return escapeHtml(item.preview_emoji || '⬛');
}

function renderQueue() {
  const body = qs('queueBody');
  const q = qs('queueSearch').value.trim().toLowerCase();

  const filtered = queueItems.filter(item => {
    if (!q) return true;
    return (
      (item.name || '').toLowerCase().includes(q) ||
      (item.source_filename || '').toLowerCase().includes(q) ||
      (item.material || '').toLowerCase().includes(q) ||
      (item.brand || '').toLowerCase().includes(q) ||
      (item.color || '').toLowerCase().includes(q) ||
      (item.printer || '').toLowerCase().includes(q) ||
      (item.item_type || '').toLowerCase().includes(q) ||
      (item.speed_label || '').toLowerCase().includes(q)
    );
  });

  if (!filtered.length) {
    body.innerHTML = `<tr><td colspan="10" class="empty">No queue items found</td></tr>`;
    return;
  }

  body.innerHTML = filtered.map(item => {
    const isSwap = (item.item_type || 'print') === 'filament_swap';
    const colorStyle = colorDotStyle(item.color);
    const errorSub = item.last_error ? `<div class="sub" style="color:#b42318">${escapeHtml(item.last_error)}</div>` : '';
    const swapSub = isSwap ? `<div class="swap-note">Queue pauses here until you press Continue</div>` : '';
    const speedSub = !isSwap ? `<div class="sub">Speed: ${escapeHtml(item.speed_label || 'Standard')}</div>` : '';
    const brandSub = !isSwap ? `<div class="sub">Brand: ${escapeHtml(item.brand || 'Generic')}</div>` : '';
    const locked = item.is_current || (String(item.status).toLowerCase() === 'printing') || (String(item.status).toLowerCase() === 'starting') || (String(item.status).toLowerCase() === 'waiting');
    const timer = getQueueTimerDisplay(item);

    return `
      <tr class="${isSwap ? 'swap-row' : ''}">
        <td><div class="preview">${getPreviewHtml(item)}</div></td>
        <td class="name-cell">
          <div class="name">${escapeHtml(item.name)}</div>
          <div class="sub">${escapeHtml(item.source_filename || '')}</div>
          ${brandSub}
          ${speedSub}
          ${swapSub}
          ${errorSub}
        </td>
        <td>${escapeHtml(item.printer || '-')}</td>
        <td>${isSwap ? '-' : escapeHtml(item.material + ((item.brand && item.brand !== 'Generic') ? ' • ' + item.brand : ''))}</td>
        <td>${isSwap ? '-' : `<span class="material-dot" style="background:${colorStyle}"></span>${escapeHtml(item.color || '-')}`}</td>
        <td class="timer-cell">
          <div class="big">${escapeHtml(timer.main)}</div>
          <div class="sub">${escapeHtml(timer.sub)}</div>
        </td>
        <td>${escapeHtml(item.repetitions_label || '-')}</td>
        <td>${escapeHtml(item.automatic_print_ejection || '-')}</td>
        <td>${statusBadge(item)}</td>
        <td>
          <div class="row-actions">
            <button class="icon-btn" title="Move up" onclick="moveQueue('${item.id}','up','${escapeHtml(item.name).replaceAll("'", "\\'")}')" ${locked ? 'disabled' : ''}>↑</button>
            <button class="icon-btn" title="Move down" onclick="moveQueue('${item.id}','down','${escapeHtml(item.name).replaceAll("'", "\\'")}')" ${locked ? 'disabled' : ''}>↓</button>
            ${!isSwap ? `<button class="icon-btn wide" title="Add filament swap after this task" onclick="addSwapAfter('${item.id}','${escapeHtml(item.name).replaceAll("'", "\\'")}')" ${locked ? 'disabled' : ''}>+ Swap</button>` : ''}
            ${isSwap && item.status === 'waiting' ? `<button class="icon-btn wide" title="Continue after swap" onclick="continueSpecificSwap('${item.id}')">Continue</button>` : ''}
            <button class="icon-btn" title="Delete" onclick="deleteQueue('${item.id}','${escapeHtml(item.name).replaceAll("'", "\\'")}')" ${locked ? 'disabled' : ''}>🗑</button>
          </div>
        </td>
      </tr>
    `;
  }).join('');
}

function updateSwapModal() {
  const modal = qs('swapModalBackdrop');
  const task = qs('swapModalTask');

  if (printerStatus.manual_swap_active && activeFilamentSwapId) {
    const item = queueItems.find(x => x.id === activeFilamentSwapId);
    const name = item ? item.name : 'Filament swap';
    task.textContent = `${name} is waiting. Swap the filament, then click Continue.`;
    modal.classList.add('show');
  } else {
    modal.classList.remove('show');
  }
}

async function reloadQueue(){
  try{
    const r = await fetch('/api/queue');
    const j = await r.json();
    queueItems = j.items || [];
    queueAutorunEnabled = !!j.autorun_enabled;
    currentQueueId = j.current_item_id || null;
    activeFilamentSwapId = j.active_filament_swap_id || null;
    printerOptions = j.printer_options || printerOptions;
    qs('autorunBtn').textContent = queueAutorunEnabled ? 'Autorun On' : 'Autorun Off';
    qs('autorunBtn').className = queueAutorunEnabled ? 'btn btn-green' : 'btn btn-white';
    renderBuilderPrinterOptions();
    renderQueue();
    updateSwapModal();
  }catch(e){
    addLog('Queue reload failed: ' + e);
  }
}

async function manualReloadQueue(){
  await reloadQueue();
  showToast('Queue reloaded', 'The queue list was refreshed.', 'success');
}

async function moveQueue(id, direction, itemName='Queue item'){
  const dirLabel = direction === 'up' ? 'up' : 'down';
  await postJson('/api/queue/move', {id, direction}, {
    toastTitle: `${itemName} moved ${dirLabel}`
  });
}

async function deleteQueue(id, itemName='Queue item'){
  await postJson('/api/queue/delete', {id}, {
    toastTitle: `${itemName} removed`
  });
}

async function toggleAutorun(){
  await postJson('/api/queue/autorun', {enabled: !queueAutorunEnabled}, {
    toastTitle: !queueAutorunEnabled ? 'Autorun enabled' : 'Autorun disabled'
  });
}

async function addSwapAfter(afterId, itemName='Queue item'){
  await postJson('/api/queue/add_filament_swap', {after_id: afterId}, {
    toastTitle: `Swap added after ${itemName}`
  });
}

async function addSwapAtEnd(){
  await postJson('/api/queue/add_filament_swap', {}, {
    toastTitle: 'Filament swap added'
  });
}

async function continueSpecificSwap(id){
  await postJson('/api/queue/continue_filament_swap', {id}, {
    toastTitle: 'Filament swap completed'
  });
}

async function continueFilamentSwap(){
  await postJson('/api/queue/continue_filament_swap', {}, {
    toastTitle: 'Filament swap completed'
  });
}

async function refreshStatus(){
  try{
    const query = selectedMachinePrinterId ? `?printer_id=${encodeURIComponent(selectedMachinePrinterId)}` : '';
    const r = await fetch('/api/status' + query);
    const s = await r.json();
    printerStatus = s;
    printerStates = s.printers || [];
    selectedMachinePrinterId = s.selected_printer_id || selectedMachinePrinterId;

    qs('state').textContent = s.gcode_state || 'Unknown';
    qs('percent').textContent = s.mc_percent ?? 0;
    qs('percentBar').style.width = `${Math.max(0, Math.min(100, Number(s.mc_percent || 0)))}%`;
    qs('layer').textContent = `${s.layer_num ?? 0} / ${s.total_layer_num ?? 0}`;
    qs('nozzle').textContent = `${s.nozzle_temper ?? 0} / ${s.nozzle_target_temper ?? 0} °C`;
    qs('bed').textContent = `${s.bed_temper ?? 0} / ${s.bed_target_temper ?? 0} °C`;
    qs('timeleft').textContent = s.remaining_time_str || '-';

    if (qs('topSubtitle')) {
      qs('topSubtitle').textContent = s.printer_name
        ? `${s.printer_name} • ${APP_NAME} • ${s.connected ? 'Connected' : 'Disconnected'}`
        : APP_TAGLINE;
    }

    renderMachinePrinterSelect();
    renderPrinterSummaryGrid();
    renderCamera();
    updateSwapModal();
    renderQueue();
  }catch(e){
    addLog('Status refresh failed: ' + e);
  }
}

async function manualRefreshStatus(){
  const result = await postJson('/api/refresh', {}, {
    toastTitle: 'Status refresh requested'
  });
  if (result && result.ok) {
    await refreshStatus();
  }
}

qs('buildPrintForm').onsubmit = async (e) => {
  e.preventDefault();
  try{
    const fd = new FormData(e.target);
    const file = fd.get('file');
    const fileName = file && file.name ? file.name : 'File';

    const r = await fetch('/api/build_print', {method:'POST', body:fd});
    const j = await r.json();

    addLog(j.message || JSON.stringify(j));
    showToast(
      j.ok ? `${fileName} added to queue` : `${fileName} failed`,
      j.message || '',
      j.ok ? 'success' : 'error',
      j.ok ? 3400 : 4500
    );

    if (j.ok) {
      e.target.reset();
      qs('detectedTimeInfo').textContent = 'Auto detect waits for a file.';
    }
    refreshStatus();
    reloadQueue();
  }catch(err){
    addLog('Build add failed: ' + err);
    showToast('Add to queue failed', String(err), 'error', 4500);
  }
};

qs('sourceFileInput').addEventListener('change', async (e) => {
  const file = e.target.files?.[0];
  const info = qs('detectedTimeInfo');
  if (!file) {
    info.textContent = 'Auto detect waits for a file.';
    return;
  }

  info.textContent = 'Detecting estimated time...';
  try {
    const fd = new FormData();
    fd.append('file', file);
    const r = await fetch('/api/detect_time', {method:'POST', body:fd});
    const j = await r.json();
    if (j.ok && Number(j.minutes_per_copy) > 0) {
      if (Number(qs('minutesOverride').value || 0) === 0) {
        qs('minutesOverride').value = Number(j.minutes_per_copy);
      }
      info.textContent = `Detected about ${j.minutes_per_copy} min per copy.`;
      showToast('Print time detected', `${file.name}: about ${j.minutes_per_copy} min per copy.`, 'info', 2600);
    } else {
      info.textContent = 'Could not auto detect. Default will be used unless you set a value.';
      showToast('Print time not detected', j.message || 'Could not auto detect print time.', 'warning', 3200);
    }
  } catch (err) {
    info.textContent = 'Time detect failed.';
    addLog('Time detect failed: ' + err);
    showToast('Time detection failed', String(err), 'error', 4200);
  }
});

async function loadLogs() {
  try {
    const r = await fetch('/api/logs');
    const items = await r.json();
    for (const x of items) addLog(x);
  } catch (e) {
    addLog('Could not load logs: ' + e);
  }
}

window.addEventListener('click', (e) => {
  if (e.target === qs('speedModalBackdrop')) closeSpeedModal();
  if (e.target === qs('swapModalBackdrop')) qs('swapModalBackdrop').classList.remove('show');
  if (e.target === qs('settingsModalBackdrop')) closeSettingsModal();
});

document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape') {
    closeSpeedModal();
    closeSettingsModal();
  }
});

setInterval(refreshStatus, 2500);
setInterval(loadLogs, 2000);
setInterval(reloadQueue, 3500);

loadPrinters();
refreshStatus();
reloadQueue();
loadLogs();
renderCamera();
</script>
</body>
</html>
"""


def sanitize_filename(name: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9._ -]", "_", os.path.basename(name))
    if not safe.lower().endswith(".3mf"):
        safe += ".3mf"
    return safe


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def parse_iso(ts: str):
    try:
        if ts:
            return datetime.fromisoformat(ts)
    except Exception:
        return None
    return None


def format_seconds_human(total_seconds: int) -> str:
    total_seconds = max(0, int(total_seconds or 0))
    h = total_seconds // 3600
    m = (total_seconds % 3600) // 60
    s = total_seconds % 60
    if h > 0:
        return f"{h}h {m}m {s}s"
    if m > 0:
        return f"{m}m {s}s"
    return f"{s}s"


def estimate_duration(minutes_per_copy: int, copies: int) -> str:
    total_seconds = max(0, int(minutes_per_copy or 0)) * 60 * max(1, int(copies or 1))
    return format_seconds_human(total_seconds)


def speed_throughput_multiplier(level) -> float:
    return float(SPEED_THROUGHPUT_MULTIPLIERS.get(normalize_speed_level(level), 1.0))


def scale_estimated_seconds_for_speed(base_seconds: int, speed_level) -> int:
    base_seconds = max(0, int(base_seconds or 0))
    if base_seconds <= 0:
        return 0

    throughput = speed_throughput_multiplier(speed_level)
    if throughput <= 0:
        throughput = 1.0

    return max(1, int(round(base_seconds / throughput)))


def estimate_current_copy_index(item: dict, remaining_time_seconds=None, progress_percent=None, reference_time=None) -> int:
    copies = max(1, int(item.get("copies", 1) or 1))
    if copies <= 1:
        return 1

    estimated_per_copy = max(0, int(item.get("estimated_seconds_per_copy", 0) or 0))
    estimated_total = max(0, int(item.get("estimated_total_seconds", 0) or 0))

    if progress_percent is not None:
        try:
            ratio = max(0.0, min(1.0, float(progress_percent) / 100.0))
        except Exception:
            ratio = 0.0
        if ratio > 0:
            return max(1, min(copies, int(ratio * copies) + 1))

    if estimated_per_copy > 0:
        started_at = parse_iso(str(item.get("started_at", "") or ""))
        if started_at:
            now_dt = reference_time if reference_time is not None else datetime.now()
            elapsed = max(0, int((now_dt - started_at).total_seconds()))
            return max(1, min(copies, int(elapsed // estimated_per_copy) + 1))

    if estimated_per_copy > 0 and estimated_total > 0 and remaining_time_seconds is not None:
        remaining = max(0, min(estimated_total, int(remaining_time_seconds or 0)))
        if 0 < remaining < estimated_total:
            elapsed = max(0, estimated_total - remaining)
            return max(1, min(copies, int(elapsed // estimated_per_copy) + 1))

    return 1


def build_repetitions_label(item: dict, is_current: bool, printer_status: dict) -> str:
    copies = max(1, int(item.get("copies", 1) or 1))
    if copies <= 1:
        return "1 of 1"

    status = str(item.get("status", "")).lower()
    if status == "done":
        return f"{copies} of {copies}"

    if is_current or status == "printing":
        current_copy = estimate_current_copy_index(
            item,
            remaining_time_seconds=printer_status.get("remaining_time", 0),
            progress_percent=printer_status.get("mc_percent", 0),
        )
        return f"{current_copy} of {copies}"

    if status == "starting":
        return f"1 of {copies}"

    if status == "queued":
        return f"0 of {copies}"

    return str(item.get("repetitions_label", "") or f"1 of {copies}")


def parse_options_json(raw_value) -> dict:
    try:
        return json.loads(raw_value or "{}")
    except Exception:
        return {}


def refresh_queue_item_timing_from_file(item: dict) -> dict:
    d = dict(item)
    if str(d.get("item_type", QUEUE_ITEM_TYPE_PRINT) or QUEUE_ITEM_TYPE_PRINT) != QUEUE_ITEM_TYPE_PRINT:
        return d

    copies = max(1, int(d.get("copies", 1) or 1))
    opts = parse_options_json(d.get("options_json", "{}"))
    speed_level = normalize_speed_level(opts.get("speed_level", SPEED_STANDARD))

    try:
        manual_minutes = int(opts.get("minutes_per_copy_override", 0) or 0)
    except Exception:
        manual_minutes = 0

    detected_minutes = 0
    if manual_minutes > 0:
        detected_minutes = manual_minutes
    else:
        file_path = Path(str(d.get("file_path", "") or "").strip())
        if not file_path.exists() or file_path.suffix.lower() != ".3mf":
            return d

        try:
            detected_minutes = int(utility_printer().detect_minutes_per_copy_from_3mf(file_path.read_bytes()) or 0)
        except Exception:
            return d

    if detected_minutes <= 0:
        return d

    detected_per_copy = scale_estimated_seconds_for_speed(detected_minutes * 60, speed_level)
    detected_total = detected_per_copy * copies
    current_per_copy = int(d.get("estimated_seconds_per_copy", 0) or 0)
    current_total = int(d.get("estimated_total_seconds", 0) or 0)
    current_duration = str(d.get("duration", "") or "")
    detected_duration = format_seconds_human(detected_total)

    if (
        current_per_copy == detected_per_copy
        and current_total == detected_total
        and current_duration == detected_duration
    ):
        return d

    d["estimated_seconds_per_copy"] = detected_per_copy
    d["estimated_total_seconds"] = detected_total
    d["duration"] = detected_duration

    queue_update(
        d["id"],
        estimated_seconds_per_copy=detected_per_copy,
        estimated_total_seconds=detected_total,
        duration=detected_duration,
    )
    return d


def make_unique_flowq_filename(original_filename: str, copies: int) -> str:
    base = os.path.basename(original_filename)
    base = re.sub(r"(\.gcode)?\.3mf$", "", base, flags=re.IGNORECASE)
    unique_tag = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:8]
    return f"{base}_{APP_FILE_TAG}_{copies}x_{unique_tag}.gcode.3mf"


def preview_filename_for_item(item_id: str) -> str:
    return f"{item_id}.png"


def db_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def ensure_queue_column(conn, column_name: str, ddl: str):
    cols = {row["name"] for row in conn.execute("PRAGMA table_info(queue_items)").fetchall()}
    if column_name not in cols:
        conn.execute(f"ALTER TABLE queue_items ADD COLUMN {ddl}")


def init_db():
    with db_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS queue_items (
                id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                position INTEGER NOT NULL,
                name TEXT NOT NULL,
                source_filename TEXT NOT NULL,
                generated_filename TEXT NOT NULL,
                file_path TEXT NOT NULL,
                copies INTEGER NOT NULL DEFAULT 1,
                repetitions_label TEXT NOT NULL DEFAULT '',
                repetition_method TEXT NOT NULL DEFAULT '',
                automatic_print_ejection TEXT NOT NULL DEFAULT '',
                material TEXT NOT NULL DEFAULT '',
                brand TEXT NOT NULL DEFAULT 'Generic',
                color TEXT NOT NULL DEFAULT '',
                printer TEXT NOT NULL DEFAULT 'Bambu P1S',
                duration TEXT NOT NULL DEFAULT '-',
                status TEXT NOT NULL DEFAULT 'queued',
                last_error TEXT NOT NULL DEFAULT '',
                preview_emoji TEXT NOT NULL DEFAULT '⬛',
                preview_path TEXT NOT NULL DEFAULT '',
                options_json TEXT NOT NULL DEFAULT '{}',
                estimated_seconds_per_copy INTEGER NOT NULL DEFAULT 0,
                estimated_total_seconds INTEGER NOT NULL DEFAULT 0,
                actual_total_seconds INTEGER NOT NULL DEFAULT 0,
                started_at TEXT NOT NULL DEFAULT '',
                finished_at TEXT NOT NULL DEFAULT '',
                item_type TEXT NOT NULL DEFAULT 'print',
                swap_message TEXT NOT NULL DEFAULT 'Please swap the filament and click continue.',
                target_printer_id TEXT NOT NULL DEFAULT '',
                assigned_printer_id TEXT NOT NULL DEFAULT ''
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS printers (
                id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                position INTEGER NOT NULL,
                name TEXT NOT NULL,
                model TEXT NOT NULL DEFAULT 'Bambu P1S',
                ip TEXT NOT NULL,
                access_code TEXT NOT NULL,
                serial TEXT NOT NULL,
                camera_url TEXT NOT NULL DEFAULT '',
                enabled INTEGER NOT NULL DEFAULT 1
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS app_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )

        ensure_queue_column(conn, "preview_path", "preview_path TEXT NOT NULL DEFAULT ''")
        ensure_queue_column(conn, "item_type", f"item_type TEXT NOT NULL DEFAULT '{QUEUE_ITEM_TYPE_PRINT}'")
        ensure_queue_column(conn, "swap_message", "swap_message TEXT NOT NULL DEFAULT 'Please swap the filament and click continue.'")
        ensure_queue_column(conn, "brand", "brand TEXT NOT NULL DEFAULT 'Generic'")
        ensure_queue_column(conn, "target_printer_id", "target_printer_id TEXT NOT NULL DEFAULT ''")
        ensure_queue_column(conn, "assigned_printer_id", "assigned_printer_id TEXT NOT NULL DEFAULT ''")

        conn.execute("CREATE INDEX IF NOT EXISTS idx_queue_items_position ON queue_items(position)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_queue_items_status ON queue_items(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_queue_items_target_printer ON queue_items(target_printer_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_queue_items_assigned_printer ON queue_items(assigned_printer_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_printers_position ON printers(position)")
        conn.commit()


def state_get(key: str, default: str = "") -> str:
    with db_conn() as conn:
        row = conn.execute("SELECT value FROM app_state WHERE key = ?", (key,)).fetchone()
    if not row:
        return default
    return str(row["value"])


def state_set(key: str, value) -> None:
    with db_conn() as conn:
        conn.execute(
            """
            INSERT INTO app_state(key, value)
            VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, str(value)),
        )
        conn.commit()


def state_set_many(values: dict) -> None:
    with db_conn() as conn:
        for key, value in values.items():
            conn.execute(
                """
                INSERT INTO app_state(key, value)
                VALUES(?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, str(value)),
            )
        conn.commit()


def state_bool_get(key: str, default: bool = False) -> bool:
    raw = state_get(key, "1" if default else "0").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def printer_list():
    with db_conn() as conn:
        rows = conn.execute("SELECT * FROM printers WHERE enabled = 1 ORDER BY position ASC, created_at ASC").fetchall()
    return [dict(r) for r in rows]


def printer_get(printer_id: str):
    with db_conn() as conn:
        row = conn.execute("SELECT * FROM printers WHERE id = ?", (printer_id,)).fetchone()
    return dict(row) if row else None


def printer_find_by_ip(ip: str):
    target = str(ip or "").strip()
    if not target:
        return None
    with db_conn() as conn:
        row = conn.execute("SELECT * FROM printers WHERE enabled = 1 AND ip = ?", (target,)).fetchone()
    return dict(row) if row else None


def printer_find_by_serial(serial: str):
    target = str(serial or "").strip().lower()
    if not target:
        return None
    with db_conn() as conn:
        row = conn.execute("SELECT * FROM printers WHERE enabled = 1 AND lower(serial) = ?", (target,)).fetchone()
    return dict(row) if row else None


def printer_next_position() -> int:
    with db_conn() as conn:
        row = conn.execute("SELECT COALESCE(MAX(position), 0) AS max_pos FROM printers").fetchone()
    return int((row["max_pos"] if row else 0) or 0) + 1


def printer_insert(name: str, ip: str, access_code: str, serial: str, camera_url: str = ""):
    item_id = str(uuid.uuid4())
    now = now_iso()
    with db_conn() as conn:
        row = conn.execute("SELECT COALESCE(MAX(position), 0) AS max_pos FROM printers").fetchone()
        next_position = int((row["max_pos"] if row else 0) or 0) + 1
        conn.execute(
            """
            INSERT INTO printers (
                id, created_at, updated_at, position, name, model, ip, access_code, serial, camera_url, enabled
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item_id,
                now,
                now,
                next_position,
                name,
                DEFAULT_PRINTER_MODEL,
                ip,
                access_code,
                serial,
                camera_url,
                1,
            ),
        )
        conn.commit()
    return item_id


def selected_machine_printer_id():
    printers = printer_list()
    selected_id = state_get("selected_machine_printer_id", "").strip()
    valid_ids = {p["id"] for p in printers}
    if selected_id in valid_ids:
        return selected_id
    fallback_id = printers[0]["id"] if printers else None
    state_set("selected_machine_printer_id", fallback_id or "")
    return fallback_id


def set_selected_machine_printer_id(printer_id: str | None):
    state_set("selected_machine_printer_id", str(printer_id or ""))


def printer_choice_options():
    printers = printer_list()
    options = [{"value": "", "label": FIRST_AVAILABLE_LABEL}]
    for printer in printers:
        options.append({"value": printer["id"], "label": printer["name"]})
    return options


def printer_name_by_id(printer_id: str) -> str:
    printer = printer_get(printer_id)
    if not printer:
        return "Unknown printer"
    return str(printer.get("name", "") or "Printer")


def queue_target_printer_id(item: dict) -> str:
    return str(item.get("target_printer_id", "") or "").strip()


def queue_assigned_printer_id(item: dict) -> str:
    return str(item.get("assigned_printer_id", "") or "").strip()


def queue_item_printer_label(item: dict) -> str:
    if str(item.get("item_type", QUEUE_ITEM_TYPE_PRINT) or QUEUE_ITEM_TYPE_PRINT) != QUEUE_ITEM_TYPE_PRINT:
        return "Queue barrier"

    assigned_id = queue_assigned_printer_id(item)
    if assigned_id:
        return printer_name_by_id(assigned_id)

    target_id = queue_target_printer_id(item)
    if target_id:
        return printer_name_by_id(target_id)

    return FIRST_AVAILABLE_LABEL


def queue_item_matches_printer(item: dict, printer_id: str) -> bool:
    if str(item.get("item_type", QUEUE_ITEM_TYPE_PRINT) or QUEUE_ITEM_TYPE_PRINT) != QUEUE_ITEM_TYPE_PRINT:
        return False

    assigned_id = queue_assigned_printer_id(item)
    if assigned_id:
        return assigned_id == printer_id

    target_id = queue_target_printer_id(item)
    if not target_id:
        return True

    return target_id == printer_id


def queue_list():
    with db_conn() as conn:
        rows = conn.execute("SELECT * FROM queue_items ORDER BY position ASC, created_at ASC").fetchall()
    return [dict(r) for r in rows]


def queue_get(item_id: str):
    with db_conn() as conn:
        row = conn.execute("SELECT * FROM queue_items WHERE id = ?", (item_id,)).fetchone()
    return dict(row) if row else None


def queue_next_position() -> int:
    with db_conn() as conn:
        row = conn.execute("SELECT COALESCE(MAX(position), 0) AS max_pos FROM queue_items").fetchone()
    return int((row["max_pos"] if row else 0) or 0) + 1


def queue_normalize_positions(conn=None):
    owns_conn = conn is None
    if owns_conn:
        conn = db_conn()
    try:
        rows = conn.execute("SELECT id FROM queue_items ORDER BY position ASC, created_at ASC").fetchall()
        for idx, row in enumerate(rows, start=1):
            conn.execute("UPDATE queue_items SET position = ?, updated_at = ? WHERE id = ?", (idx, now_iso(), row["id"]))
        if owns_conn:
            conn.commit()
    finally:
        if owns_conn:
            conn.close()


def queue_insert(item: dict):
    with db_conn() as conn:
        conn.execute(
            """
            INSERT INTO queue_items (
                id, created_at, updated_at, position, name, source_filename, generated_filename,
                file_path, copies, repetitions_label, repetition_method, automatic_print_ejection,
                material, brand, color, printer, duration, status, last_error, preview_emoji, preview_path,
                options_json, estimated_seconds_per_copy, estimated_total_seconds,
                actual_total_seconds, started_at, finished_at, item_type, swap_message,
                target_printer_id, assigned_printer_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item["id"],
                item["created_at"],
                item["updated_at"],
                int(item["position"]),
                item["name"],
                item["source_filename"],
                item["generated_filename"],
                item["file_path"],
                int(item.get("copies", 1)),
                item.get("repetitions_label", "1 of 1"),
                item.get("repetition_method", ""),
                item.get("automatic_print_ejection", AUTO_EJECT_NONE),
                item.get("material", "Generic"),
                item.get("brand", "Generic"),
                item.get("color", "Black"),
                item.get("printer", "Bambu P1S"),
                item.get("duration", "-"),
                item.get("status", "queued"),
                item.get("last_error", ""),
                item.get("preview_emoji", "⬛"),
                item.get("preview_path", ""),
                item.get("options_json", "{}"),
                int(item.get("estimated_seconds_per_copy", 0) or 0),
                int(item.get("estimated_total_seconds", 0) or 0),
                int(item.get("actual_total_seconds", 0) or 0),
                item.get("started_at", ""),
                item.get("finished_at", ""),
                item.get("item_type", QUEUE_ITEM_TYPE_PRINT),
                item.get("swap_message", "Please swap the filament and click continue."),
                item.get("target_printer_id", ""),
                item.get("assigned_printer_id", ""),
            ),
        )
        conn.commit()


def generated_file_is_still_used(file_path: str, exclude_item_id=None) -> bool:
    file_path = str(file_path or "").strip()
    if not file_path:
        return False

    with db_conn() as conn:
        if exclude_item_id:
            row = conn.execute(
                "SELECT COUNT(*) AS cnt FROM queue_items WHERE file_path = ? AND id != ?",
                (file_path, exclude_item_id),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT COUNT(*) AS cnt FROM queue_items WHERE file_path = ?",
                (file_path,),
            ).fetchone()

    count = int((row["cnt"] if row else 0) or 0)
    return count > 0


def safe_delete_path(file_path: str) -> bool:
    file_path = str(file_path or "").strip()
    if not file_path:
        return False
    try:
        path = Path(file_path)
        if path.exists():
            path.unlink()
            return True
    except Exception:
        return False
    return False


def safe_delete_generated_file(file_path: str, exclude_item_id=None) -> bool:
    file_path = str(file_path or "").strip()
    if not file_path:
        return False

    if generated_file_is_still_used(file_path, exclude_item_id=exclude_item_id):
        return False

    return safe_delete_path(file_path)


def queue_delete(item_id: str):
    item = queue_get(item_id)

    with db_conn() as conn:
        conn.execute("DELETE FROM queue_items WHERE id = ?", (item_id,))
        queue_normalize_positions(conn)
        conn.commit()

    if item:
        if str(item.get("item_type", QUEUE_ITEM_TYPE_PRINT)) == QUEUE_ITEM_TYPE_PRINT:
            safe_delete_generated_file(str(item.get("file_path", "") or ""), exclude_item_id=item_id)
            safe_delete_path(str(item.get("preview_path", "") or ""))


def queue_reorder(item_id: str, direction: str):
    with db_conn() as conn:
        rows = conn.execute("SELECT id, position FROM queue_items ORDER BY position ASC, created_at ASC").fetchall()
        ids = [r["id"] for r in rows]
        if item_id not in ids:
            return False
        idx = ids.index(item_id)
        if direction == "up" and idx > 0:
            ids[idx - 1], ids[idx] = ids[idx], ids[idx - 1]
        elif direction == "down" and idx < len(ids) - 1:
            ids[idx + 1], ids[idx] = ids[idx], ids[idx + 1]
        else:
            return False
        for pos, row_id in enumerate(ids, start=1):
            conn.execute("UPDATE queue_items SET position = ?, updated_at = ? WHERE id = ?", (pos, now_iso(), row_id))
        conn.commit()
        return True


def queue_update(item_id: str, **fields):
    if not fields:
        return
    fields["updated_at"] = now_iso()
    columns = ", ".join(f"{k} = ?" for k in fields.keys())
    values = list(fields.values()) + [item_id]
    with db_conn() as conn:
        conn.execute(f"UPDATE queue_items SET {columns} WHERE id = ?", values)
        conn.commit()


def queue_first_pending():
    with db_conn() as conn:
        row = conn.execute(
            "SELECT * FROM queue_items WHERE status = 'queued' ORDER BY position ASC, created_at ASC LIMIT 1"
        ).fetchone()
    return dict(row) if row else None


def queue_mark_starting(item_id: str):
    queue_update(item_id, status="starting", last_error="")


def queue_mark_started(item_id: str):
    item = queue_get(item_id)
    started_at = item.get("started_at") if item else ""
    if not started_at:
        started_at = now_iso()

    queue_update(item_id, status="printing", started_at=started_at, finished_at="", last_error="")


def queue_mark_finished(item_id: str):
    item = queue_get(item_id)
    finished_at = now_iso()
    actual_total_seconds = 0
    if item:
        started_dt = parse_iso(item.get("started_at", ""))
        finished_dt = parse_iso(finished_at)
        if started_dt and finished_dt:
            actual_total_seconds = max(0, int((finished_dt - started_dt).total_seconds()))
    queue_update(item_id, status="done", finished_at=finished_at, actual_total_seconds=actual_total_seconds, last_error="")


def queue_mark_error(item_id: str, error_text: str):
    queue_update(item_id, status="error", last_error=str(error_text or "Unknown error"))


def queue_mark_requeued(item_id: str, reason: str = ""):
    queue_update(
        item_id,
        status="queued",
        last_error=str(reason or ""),
        started_at="",
        finished_at="",
        actual_total_seconds=0,
        assigned_printer_id="",
    )


def queue_reset_stale_starting_items():
    with db_conn() as conn:
        conn.execute(
            """
            UPDATE queue_items
            SET status = 'queued',
                last_error = CASE
                    WHEN COALESCE(last_error, '') = '' THEN 'Recovered after app restart'
                    ELSE last_error
                END,
                updated_at = ?
            WHERE status = 'starting'
            """,
            (now_iso(),),
        )
        queue_normalize_positions(conn)
        conn.commit()


def queue_insert_filament_swap(after_item_id: str | None = None, message: str = "Please swap the filament and click continue."):
    with db_conn() as conn:
        rows = conn.execute("SELECT id, position FROM queue_items ORDER BY position ASC, created_at ASC").fetchall()
        insert_position = len(rows) + 1

        if after_item_id:
            ref = conn.execute("SELECT position FROM queue_items WHERE id = ?", (after_item_id,)).fetchone()
            if not ref:
                raise ValueError("Queue item not found")
            insert_position = int(ref["position"]) + 1
            conn.execute(
                "UPDATE queue_items SET position = position + 1, updated_at = ? WHERE position >= ?",
                (now_iso(), insert_position),
            )

        item_id = str(uuid.uuid4())
        conn.execute(
            """
            INSERT INTO queue_items (
                id, created_at, updated_at, position, name, source_filename, generated_filename,
                file_path, copies, repetitions_label, repetition_method, automatic_print_ejection,
                material, brand, color, printer, duration, status, last_error, preview_emoji, preview_path,
                options_json, estimated_seconds_per_copy, estimated_total_seconds,
                actual_total_seconds, started_at, finished_at, item_type, swap_message,
                target_printer_id, assigned_printer_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item_id,
                now_iso(),
                now_iso(),
                insert_position,
                "Filament swap pause",
                "Manual stop",
                "",
                "",
                1,
                "-",
                "Manual",
                "-",
                "",
                "Generic",
                "",
                "Bambu P1S",
                "Manual",
                "queued",
                "",
                "🧵",
                "",
                "{}",
                0,
                0,
                0,
                "",
                "",
                QUEUE_ITEM_TYPE_FILAMENT_SWAP,
                message,
                "",
                "",
            ),
        )
        queue_normalize_positions(conn)
        conn.commit()
        return item_id


def queue_waiting_filament_swap():
    with db_conn() as conn:
        row = conn.execute(
            "SELECT * FROM queue_items WHERE item_type = ? AND status = 'waiting' ORDER BY position ASC LIMIT 1",
            (QUEUE_ITEM_TYPE_FILAMENT_SWAP,),
        ).fetchone()
    return dict(row) if row else None


def normalize_material_choice(material: str) -> str:
    material = str(material or "").strip()
    return material if material in MATERIAL_OPTIONS else "Generic"


def normalize_brand_choice(brand: str) -> str:
    brand = str(brand or "").strip()
    return brand if brand in BRAND_OPTIONS else "Generic"


def normalize_color_choice(color: str) -> str:
    color = str(color or "").strip()
    return color if color in COLOR_OPTIONS else "Custom"


def color_name_to_hex(color: str):
    normalized = normalize_color_choice(color).strip().lower()
    if normalized == "custom":
        return None
    return COLOR_HEX_MAP.get(normalized)


def brand_to_vendor_label(brand: str) -> str:
    normalized = normalize_brand_choice(brand)
    if normalized == "Generic":
        return "Generic"
    return BRAND_VENDOR_MAP.get(normalized.lower(), normalized)


def material_profile_for_choice(material: str) -> dict:
    normalized = normalize_material_choice(material)
    return dict(MATERIAL_PROFILE_PRESETS.get(normalized, MATERIAL_PROFILE_PRESETS["Generic"]))


def options_from_form(form) -> dict:
    material = normalize_material_choice(form.get("material", "Generic"))
    brand = normalize_brand_choice(form.get("brand", "Generic"))
    color = normalize_color_choice(form.get("color", "Black"))

    try:
        speed_level = int(form.get("speed_level", SPEED_STANDARD))
    except Exception:
        speed_level = SPEED_STANDARD

    if speed_level not in SPEED_OPTIONS:
        speed_level = SPEED_STANDARD

    return {
        "timelapse": str(form.get("timelapse", "")).lower() in {"on", "1", "true", "yes"},
        "bed_levelling": str(form.get("bed_levelling", "")).lower() in {"on", "1", "true", "yes"},
        "flow_cali": str(form.get("flow_cali", "")).lower() in {"on", "1", "true", "yes"},
        "vibration_cali": str(form.get("vibration_cali", "")).lower() in {"on", "1", "true", "yes"},
        "layer_inspect": str(form.get("layer_inspect", "")).lower() in {"on", "1", "true", "yes"},
        "use_ams": str(form.get("use_ams", "")).lower() in {"on", "1", "true", "yes"},
        "speed_level": speed_level,
        "material": material,
        "brand": brand,
        "color": color,
    }


def normalize_speed_level(level) -> int:
    try:
        level = int(level)
    except Exception:
        level = SPEED_STANDARD

    if level not in SPEED_OPTIONS:
        return SPEED_STANDARD
    return level


def speed_label(level) -> str:
    return SPEED_OPTIONS.get(normalize_speed_level(level), SPEED_OPTIONS[SPEED_STANDARD])


def queue_set_speed_for_all_prints(level: int):
    level = normalize_speed_level(level)

    with db_conn() as conn:
        rows = conn.execute(
            "SELECT id, options_json, item_type FROM queue_items ORDER BY position ASC, created_at ASC"
        ).fetchall()

        updated = 0
        for row in rows:
            if str(row["item_type"] or QUEUE_ITEM_TYPE_PRINT) != QUEUE_ITEM_TYPE_PRINT:
                continue

            opts = parse_options_json(row["options_json"])

            opts["speed_level"] = level

            conn.execute(
                "UPDATE queue_items SET options_json = ?, updated_at = ? WHERE id = ?",
                (json.dumps(opts), now_iso(), row["id"]),
            )
            updated += 1

        conn.commit()

    return updated


def _preferred_preview_score(name: str):
    lower = name.lower()

    preferred_order = [
        "metadata/plate_1.png",
        "metadata/plate_1.jpg",
        "metadata/plate_1.jpeg",
        "metadata/plate_1_small.png",
        "metadata/plate_1_small.jpg",
        "metadata/plate_1_small.jpeg",
        "metadata/preview.png",
        "metadata/preview.jpg",
        "metadata/preview.jpeg",
        "metadata/thumbnail.png",
        "metadata/thumbnail.jpg",
        "metadata/thumbnail.jpeg",
        "thumbnails/thumbnail.png",
        "thumbnails/thumbnail.jpg",
        "thumbnails/thumbnail.jpeg",
        "3d/thumbnail.png",
        "3d/thumbnail.jpg",
        "3d/thumbnail.jpeg",
    ]

    for idx, item in enumerate(preferred_order):
        if lower == item:
            return idx

    if "plate_" in lower and lower.endswith((".png", ".jpg", ".jpeg")):
        return 100
    if lower.endswith((".png", ".jpg", ".jpeg")):
        return 200
    return 9999


def extract_preview_image_from_3mf(blob: bytes):
    try:
        with zipfile.ZipFile(io.BytesIO(blob), "r") as zf:
            names = [n for n in zf.namelist() if not n.endswith("/")]
            candidates = [n for n in names if n.lower().endswith((".png", ".jpg", ".jpeg"))]
            if not candidates:
                return None

            candidates = sorted(candidates, key=_preferred_preview_score)
            for name in candidates:
                try:
                    raw = zf.read(name)
                    img = Image.open(io.BytesIO(raw))
                    img.load()
                    return img.convert("RGBA")
                except Exception:
                    continue
    except Exception:
        return None
    return None


def generate_preview_file_for_queue_item(item_id: str, blob: bytes, title: str, material: str, color: str, preview_emoji: str):
    out_path = PREVIEW_OUTPUT_DIR / preview_filename_for_item(item_id)

    if out_path.exists() and not FORCE_REGEN_PREVIEW:
        return str(out_path)

    embedded = extract_preview_image_from_3mf(blob)
    size = 320

    if embedded is not None:
        img = embedded.convert("RGBA")

        datas = img.getdata()
        new_data = []
        for px in datas:
            if px[0] > 240 and px[1] > 240 and px[2] > 240:
                new_data.append((255, 255, 255, 0))
            else:
                new_data.append(px)
        img.putdata(new_data)

        canvas = Image.new("RGBA", (size, size), (255, 255, 255, 0))
        fitted = ImageOps.contain(img, (280, 280))
        x = (size - fitted.width) // 2
        y = (size - fitted.height) // 2
        canvas.alpha_composite(fitted, (x, y))
        final_img = canvas.convert("RGB")
    else:
        final_img = Image.new("RGB", (size, size), (245, 245, 245))
        draw = ImageDraw.Draw(final_img)
        draw.text((size // 2, size // 2), preview_emoji or "📦", fill=(60, 60, 60), anchor="mm")

    final_img.save(out_path, format="PNG", optimize=True)
    return str(out_path)


class ImplicitFTP_TLS(FTP_TLS):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._sock = None

    @property
    def sock(self):
        return self._sock

    @sock.setter
    def sock(self, value):
        if value is not None and not isinstance(value, ssl.SSLSocket):
            value = self.context.wrap_socket(value, server_hostname=getattr(self, "host", None))
        self._sock = value

    def connect(self, host="", port=0, timeout=-999):
        return super().connect(host, port, timeout)

    def ntransfercmd(self, cmd, rest=None):
        conn, size = FTP.ntransfercmd(self, cmd, rest)
        if self._prot_p:
            try:
                if isinstance(self.sock, ssl.SSLSocket) and self.sock.session is not None:
                    conn = self.context.wrap_socket(conn, server_hostname=self.host, session=self.sock.session)
                else:
                    conn = self.context.wrap_socket(conn, server_hostname=self.host)
            except TypeError:
                conn = self.context.wrap_socket(conn, server_hostname=self.host)
        return conn, size


class Bambu:
    def __init__(self, printer_config: dict):
        self.printer_id = str(printer_config.get("id", "") or "").strip()
        self.printer_name = str(printer_config.get("name", "") or "Printer").strip()
        self.printer_model = str(printer_config.get("model", "") or DEFAULT_PRINTER_MODEL).strip() or DEFAULT_PRINTER_MODEL
        self.printer_ip = str(printer_config.get("ip", "") or "").strip()
        self.access_code = str(printer_config.get("access_code", "") or "").strip()
        self.serial = str(printer_config.get("serial", "") or "").strip()
        self.camera_url = str(printer_config.get("camera_url", "") or "").strip()
        self.runtime_prefix = f"printer_runtime:{self.printer_id}:"
        self.client = None
        self.connected = False
        self.seq = 1
        self.lock = threading.Lock()
        self.logs: list[str] = []

        self.queue_autorun_enabled = state_bool_get("queue_autorun_enabled", True)
        self.queue_current_item_id = state_get(self.runtime_key("queue_current_item_id"), "").strip() or None
        self.queue_last_started_filename = state_get(self.runtime_key("queue_last_started_filename"), "").strip() or None
        self.queue_seen_running = state_bool_get(self.runtime_key("queue_seen_running"), False)
        self.queue_start_requested_at = None
        self.queue_launch_busy = False
        self.queue_retry_count = 0
        self.queue_worker_lock = threading.Lock()
        self.status_report_received = False

        self.manual_swap_active = state_bool_get("manual_swap_active", False)
        self.manual_swap_item_id = state_get("manual_swap_item_id", "").strip() or None

        self.last_command_reply = None
        self.last_command_reply_lock = threading.Lock()
        self.last_speed_apply_at = 0.0
        self.last_speed_item_id = None
        self.last_speed_level = None

        self.status = {
            "gcode_state": "Disconnected",
            "mc_percent": 0,
            "nozzle_temper": 0,
            "nozzle_target_temper": 0,
            "bed_temper": 0,
            "bed_target_temper": 0,
            "layer_num": 0,
            "total_layer_num": 0,
            "gcode_file": "-",
            "spd_lvl": "-",
            "ams_data": None,
            "remaining_time": 0,
        }

        if self.queue_current_item_id:
            self.log(
                f"Recovered runtime state: current_item={self.queue_current_item_id}, "
                f"seen_running={self.queue_seen_running}, last_file={self.queue_last_started_filename or '-'}"
            )
        if self.manual_swap_active:
            self.log(f"Recovered filament swap waiting state: {self.manual_swap_item_id or '-'}")

    def runtime_key(self, key: str) -> str:
        return f"{self.runtime_prefix}{key}"

    def sync_shared_flags(self):
        self.queue_autorun_enabled = state_bool_get("queue_autorun_enabled", True)
        self.manual_swap_active = state_bool_get("manual_swap_active", False)
        self.manual_swap_item_id = state_get("manual_swap_item_id", "").strip() or None

    def persist_runtime_state(self):
        state_set_many(
            {
                "queue_autorun_enabled": "1" if self.queue_autorun_enabled else "0",
                self.runtime_key("queue_current_item_id"): self.queue_current_item_id or "",
                self.runtime_key("queue_last_started_filename"): self.queue_last_started_filename or "",
                self.runtime_key("queue_seen_running"): "1" if self.queue_seen_running else "0",
                "manual_swap_active": "1" if self.manual_swap_active else "0",
                "manual_swap_item_id": self.manual_swap_item_id or "",
            }
        )

    def speed_level_for_queue_item(self, item) -> int:
        if not item:
            return SPEED_STANDARD

        try:
            opts = json.loads(item.get("options_json", "{}") or "{}")
        except Exception:
            opts = {}

        return normalize_speed_level(opts.get("speed_level", SPEED_STANDARD))

    def apply_speed_for_queue_item(self, item):
        if not item:
            return False

        level = self.speed_level_for_queue_item(item)

        try:
            self.speed(level)
            self.last_speed_apply_at = time.time()
            self.last_speed_item_id = item.get("id")
            self.last_speed_level = level
            self.log(f"Applied queued speed {speed_label(level)} to print {item.get('name', item.get('id', '-'))}")
            return True
        except Exception as e:
            self.log(f"Failed to apply queued speed to {item.get('name', item.get('id', '-'))}: {e}")
            return False

    def apply_speed_for_current_queue_item(self):
        if not self.queue_current_item_id:
            return False
        item = queue_get(self.queue_current_item_id)
        return self.apply_speed_for_queue_item(item)

    def apply_speed_for_current_queue_item_delayed(self, delays=(2.0, 5.0, 9.0)):
        if not self.queue_current_item_id:
            return

        item_id = self.queue_current_item_id

        def _apply_once():
            try:
                if self.queue_current_item_id != item_id:
                    return
                if not self.is_runningish():
                    return
                self.apply_speed_for_current_queue_item()
            except Exception as e:
                self.log(f"Delayed speed apply failed: {e}")

        for delay in delays:
            threading.Timer(delay, _apply_once).start()

    def maybe_enforce_current_queue_speed(self, min_interval_seconds=6.0):
        if not self.queue_current_item_id:
            return False
        if not self.is_runningish():
            return False

        item = queue_get(self.queue_current_item_id)
        if not item or str(item.get("item_type", QUEUE_ITEM_TYPE_PRINT)) != QUEUE_ITEM_TYPE_PRINT:
            return False

        desired_level = self.speed_level_for_queue_item(item)
        current_level = self.round_int(self.status.get("spd_lvl", 0), 0)

        if current_level == desired_level:
            return False

        same_target = self.last_speed_item_id == item.get("id") and self.last_speed_level == desired_level
        if same_target and (time.time() - self.last_speed_apply_at) < float(min_interval_seconds):
            return False

        self.log(
            f"Reapplying queued speed {speed_label(desired_level)} because printer reports {speed_label(current_level)}"
        )
        return self.apply_speed_for_queue_item(item)

    def log(self, msg: str) -> None:
        self.logs.append(f"[{self.printer_name}] {msg}")
        self.logs = self.logs[-500:]

    def next_seq(self) -> str:
        with self.lock:
            self.seq += 1
            return str(self.seq)

    def round_int(self, value, fallback=0):
        try:
            return int(round(float(value)))
        except Exception:
            return fallback

    def format_time(self, seconds):
        try:
            seconds = int(seconds)
            if seconds < 0:
                seconds = 0
            h = seconds // 3600
            m = (seconds % 3600) // 60
            s = seconds % 60
            if h > 0:
                return f"{h}:{m:02d}:{s:02d}"
            return f"{m}:{s:02d}"
        except Exception:
            return "-"

    def normalize_track_name(self, file_name: str) -> str:
        if not file_name:
            return ""
        name = os.path.basename(str(file_name)).strip().lower()
        name = name.split("?")[0].split("#")[0]
        changed = True
        while changed:
            changed = False
            for suffix in [".gcode.3mf", ".3mf", ".gcode"]:
                if name.endswith(suffix):
                    name = name[: -len(suffix)]
                    changed = True
                    break
        return name

    def is_same_file(self, a: str, b: str) -> bool:
        return self.normalize_track_name(a) == self.normalize_track_name(b)

    def clear_queue_runtime(self):
        self.queue_current_item_id = None
        self.queue_last_started_filename = None
        self.queue_seen_running = False
        self.queue_launch_busy = False
        self.queue_start_requested_at = None
        self.queue_retry_count = 0
        self.persist_runtime_state()

    def set_manual_swap_waiting(self, item_id: str):
        self.manual_swap_active = True
        self.manual_swap_item_id = item_id
        self.persist_runtime_state()

    def clear_manual_swap_waiting(self):
        self.manual_swap_active = False
        self.manual_swap_item_id = None
        self.persist_runtime_state()

    def connect(self):
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION1,
            client_id=f"bambu_{self.printer_id}_{int(time.time())}",
            clean_session=True,
        )
        self.client.username_pw_set("bblp", self.access_code)
        self.client.tls_set(cert_reqs=ssl.CERT_NONE, tls_version=ssl.PROTOCOL_TLS_CLIENT)
        self.client.tls_insecure_set(True)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message
        self.client.connect(self.printer_ip, MQTT_PORT, keepalive=60)
        threading.Thread(target=self.client.loop_forever, daemon=True).start()
        self.log("MQTT connect requested")

    def on_connect(self, client, userdata, flags, rc):
        self.connected = (rc == 0)
        self.status_report_received = False
        self.status["gcode_state"] = "Idle" if self.connected else f"MQTT error {rc}"
        if self.connected:
            client.subscribe(f"device/{self.serial}/report", qos=0)
            self.refresh()
        self.log(f"MQTT rc={rc}")

    def on_disconnect(self, client, userdata, rc):
        self.connected = False
        self.status_report_received = False
        self.status["gcode_state"] = "Disconnected"
        self.log(f"MQTT disconnected rc={rc}")

    def on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8", errors="ignore"))

            if "print" in payload:
                data = payload.get("print", {})
                self.status_report_received = True

                for k in (
                    "gcode_state",
                    "mc_percent",
                    "nozzle_temper",
                    "nozzle_target_temper",
                    "bed_temper",
                    "bed_target_temper",
                    "layer_num",
                    "total_layer_num",
                    "gcode_file",
                    "spd_lvl",
                ):
                    if k in data and data[k] not in (None, ""):
                        self.status[k] = data[k]

                if "ams" in data and data["ams"] not in (None, ""):
                    self.status["ams_data"] = data["ams"]

                if "mc_remaining_time" in data and data["mc_remaining_time"] not in (None, ""):
                    minutes_left = self.round_int(data["mc_remaining_time"], 0)
                    self.status["remaining_time"] = max(0, minutes_left * 60)

                for k in (
                    "nozzle_temper",
                    "nozzle_target_temper",
                    "bed_temper",
                    "bed_target_temper",
                    "mc_percent",
                    "layer_num",
                    "total_layer_num",
                ):
                    self.status[k] = self.round_int(self.status.get(k, 0), 0)

                cmd = str(data.get("command", "")).strip()
                result = str(data.get("result", "")).strip().lower()
                reason = str(data.get("reason", "")).strip()

                if cmd:
                    with self.last_command_reply_lock:
                        self.last_command_reply = {
                            "command": cmd,
                            "result": result,
                            "reason": reason,
                            "payload": data,
                            "ts": time.time(),
                        }
                    if result or reason:
                        self.log(f"MQTT reply command={cmd} result={result or '-'} reason={reason or '-'}")

                if self.is_idleish():
                    self.status["remaining_time"] = 0
                    if int(self.status.get("mc_percent", 0) or 0) >= 100:
                        self.status["mc_percent"] = 100
                    elif self.queue_current_item_id is None:
                        self.status["mc_percent"] = 0

        except Exception as e:
            self.log(f"MQTT parse failed: {e}")

    def publish(self, payload, qos=1):
        if not self.connected:
            raise RuntimeError("Printer is not connected through MQTT")
        self.client.publish(f"device/{self.serial}/request", json.dumps(payload), qos=qos)

    def refresh(self):
        self.publish({"pushing": {"sequence_id": self.next_seq(), "command": "pushall"}}, qos=0)

    def pause(self):
        self.publish({"print": {"sequence_id": self.next_seq(), "command": "pause", "param": ""}})

    def resume(self):
        self.publish({"print": {"sequence_id": self.next_seq(), "command": "resume", "param": ""}})

    def stop(self):
        self.publish({"print": {"sequence_id": self.next_seq(), "command": "stop", "param": ""}})

    def speed(self, level):
        self.publish({"print": {"sequence_id": self.next_seq(), "command": "print_speed", "param": str(level)}})

    def gcode(self, line):
        self.publish({"print": {"sequence_id": self.next_seq(), "command": "gcode_line", "param": line}})
        threading.Timer(1.0, self.refresh).start()

    def is_idleish(self) -> bool:
        state = str(self.status.get("gcode_state", "")).lower()
        return state in {"idle", "finish", "completed", "failed", "ready", "stop", "stopped", "cancel", "cancelled", "canceled", "error"}

    def is_runningish(self) -> bool:
        state = str(self.status.get("gcode_state", "")).lower()
        return state in {"running", "printing", "pause", "paused", "prepare", "preparing"}

    def is_finished_state(self) -> bool:
        state = str(self.status.get("gcode_state", "")).lower()
        return state in {"finish", "completed"}

    def was_active_print_stopped_externally(self) -> bool:
        if not self.queue_current_item_id or not self.queue_seen_running:
            return False
        if self.is_runningish() or self.is_finished_state():
            return False

        state = str(self.status.get("gcode_state", "")).lower()
        current_file = str(self.status.get("gcode_file", "")).strip()
        target_file = str(self.queue_last_started_filename or "").strip()
        interrupted_states = {"idle", "ready", "failed", "error", "stop", "stopped", "cancel", "cancelled", "canceled"}

        if state in interrupted_states:
            return True
        if not current_file:
            return True
        if target_file and not self.is_same_file(current_file, target_file):
            return True
        return False

    def active_print_stop_reason(self) -> str:
        state = str(self.status.get("gcode_state", "")).strip() or "unknown"
        current_file = str(self.status.get("gcode_file", "")).strip()
        target_file = str(self.queue_last_started_filename or "").strip()

        if not current_file:
            return f"printer cleared the active file and reported state {state}"
        if target_file and not self.is_same_file(current_file, target_file):
            return f"printer switched from {target_file} to {current_file} while reporting state {state}"
        return f"printer reported terminal state {state}"

    def find_queue_item_for_printer_file(self, file_name: str):
        target = str(file_name or "").strip()
        if not target:
            return None

        for item in queue_list():
            if str(item.get("item_type", QUEUE_ITEM_TYPE_PRINT)) != QUEUE_ITEM_TYPE_PRINT:
                continue
            if not queue_item_matches_printer(item, self.printer_id):
                continue

            candidates = [
                str(item.get("generated_filename", "") or "").strip(),
                os.path.basename(str(item.get("file_path", "") or "").strip()),
                str(item.get("source_filename", "") or "").strip(),
                str(item.get("name", "") or "").strip(),
            ]

            if any(candidate and self.is_same_file(target, candidate) for candidate in candidates):
                return item

        return None

    def effective_current_queue_item(self):
        matched_item = self.find_queue_item_for_printer_file(self.status.get("gcode_file", ""))
        if matched_item:
            return matched_item
        if self.queue_current_item_id:
            return queue_get(self.queue_current_item_id)
        return None

    def reconcile_runtime_with_printer(self):
        current_state = str(self.status.get("gcode_state", "")).lower()
        current_file = str(self.status.get("gcode_file", "")).strip()
        active_states = {"running", "printing", "pause", "paused", "prepare", "preparing"}

        if current_state not in active_states or not current_file:
            return

        matched_item = self.find_queue_item_for_printer_file(current_file)
        if matched_item:
            matched_id = matched_item["id"]
            changed_runtime = False
            cleared_ids = []

            for item in queue_list():
                if str(item.get("item_type", QUEUE_ITEM_TYPE_PRINT)) != QUEUE_ITEM_TYPE_PRINT:
                    continue
                if item["id"] == matched_id:
                    continue
                if queue_assigned_printer_id(item) != self.printer_id:
                    continue
                if str(item.get("status", "")).lower() in {"starting", "printing"}:
                    queue_mark_requeued(item["id"], "")
                    cleared_ids.append(item["id"])

            if self.queue_current_item_id != matched_id:
                previous_id = self.queue_current_item_id
                self.queue_current_item_id = matched_id
                changed_runtime = True
                self.log(
                    f"Reconciled active queue item from {previous_id or '-'} to {matched_id} using printer file {current_file}"
                )

            matched_filename = str(matched_item.get("generated_filename", "") or "").strip()
            if matched_filename and self.queue_last_started_filename != matched_filename:
                self.queue_last_started_filename = matched_filename
                changed_runtime = True

            if not self.queue_seen_running:
                self.queue_seen_running = True
                changed_runtime = True

            if queue_assigned_printer_id(matched_item) != self.printer_id:
                queue_update(matched_id, assigned_printer_id=self.printer_id)

            if str(matched_item.get("status", "")).lower() != "printing":
                queue_mark_started(matched_id)

            if cleared_ids:
                self.log(f"Cleared stale active queue items: {', '.join(cleared_ids)}")

            if changed_runtime:
                self.persist_runtime_state()
            return

        if (
            self.queue_current_item_id
            and self.queue_last_started_filename
            and not self.is_same_file(current_file, self.queue_last_started_filename)
        ):
            stale_id = self.queue_current_item_id
            stale_item = queue_get(stale_id)
            if (
                stale_item
                and queue_assigned_printer_id(stale_item) == self.printer_id
                and str(stale_item.get("status", "")).lower() in {"starting", "printing"}
            ):
                queue_mark_requeued(stale_id, "")
            self.log(
                f"Cleared stale runtime for {stale_id} because printer reports different file: {current_file}"
            )
            self.clear_queue_runtime()

    def looks_like_start_confirmed(self) -> bool:
        current_state = str(self.status.get("gcode_state", "")).lower()
        current_file = str(self.status.get("gcode_file", "")).strip()
        current_percent = int(self.status.get("mc_percent", 0) or 0)
        target_file = str(self.queue_last_started_filename or "").strip()

        preparing_states = {"prepare", "preparing", "pause", "paused"}

        if target_file and current_file:
            if self.is_same_file(current_file, target_file):
                if self.is_runningish() or current_state in preparing_states:
                    return True
            elif self.is_runningish() or current_state in preparing_states:
                return False

        if self.is_runningish():
            return True

        if (
            target_file
            and current_file
            and self.is_same_file(current_file, target_file)
            and current_state in preparing_states
        ):
            return True

        if current_percent > 0 and current_state not in {"idle", "ready", "finish", "completed", "failed"}:
            return True

        return False

    def _prepare_ftps(self):
        ftp = ImplicitFTP_TLS(timeout=FTPS_TIMEOUT)
        ftp.ssl_version = ssl.PROTOCOL_TLS_CLIENT
        ftp.context = ssl._create_unverified_context()
        ftp.context.check_hostname = False
        ftp.context.verify_mode = ssl.CERT_NONE

        self.log("FTPS connecting...")
        ftp.connect(self.printer_ip, FTPS_PORT, timeout=FTPS_TIMEOUT)
        self.log("FTPS connected")

        self.log("FTPS logging in...")
        ftp.login("bblp", self.access_code)
        self.log("FTPS logged in")

        ftp.prot_p()
        ftp.set_pasv(True)

        old_makepasv = ftp.makepasv

        def fixed_makepasv():
            host, port = old_makepasv()
            try:
                control_host = ftp.sock.getpeername()[0]
            except Exception:
                control_host = self.printer_ip
            return control_host, port

        ftp.makepasv = fixed_makepasv

        for d in ["/cache", "cache", "/"]:
            try:
                ftp.cwd(d)
                self.log(f"FTPS cwd success: {d}")
                return ftp
            except Exception:
                pass

        self.log("FTPS staying in default directory")
        return ftp

    def _remote_file_exists(self, filename):
        ftp = None
        try:
            ftp = self._prepare_ftps()
            try:
                files = ftp.nlst()
                normalized = [os.path.basename(x.rstrip('/')) for x in files]
                if filename in normalized:
                    return True
            except Exception:
                pass

            try:
                size = ftp.size(filename)
                if size is not None and int(size) > 0:
                    return True
            except Exception:
                pass

            return False
        finally:
            if ftp is not None:
                try:
                    ftp.quit()
                except Exception:
                    try:
                        ftp.close()
                    except Exception:
                        pass

    def _manual_ftps_upload(self, ftp, filename, blob):
        total = len(blob)
        sent = 0
        last_log_at = 0.0

        ftp.voidcmd("TYPE I")
        conn = ftp.transfercmd(f"STOR {filename}")

        try:
            bio = io.BytesIO(blob)
            while True:
                chunk = bio.read(FTPS_BLOCKSIZE)
                if not chunk:
                    break
                conn.sendall(chunk)
                sent += len(chunk)

                now = time.time()
                if now - last_log_at >= 0.5 or sent == total:
                    self.log(f"FTPS upload progress: {sent}/{total} bytes")
                    last_log_at = now

            try:
                conn.shutdown(1)
            except Exception:
                pass

            try:
                if isinstance(conn, ssl.SSLSocket):
                    conn.unwrap()
            except Exception:
                pass
        finally:
            try:
                conn.close()
            except Exception:
                pass

        try:
            ftp.voidresp()
            return True
        except TimeoutError:
            return False

    def ftps_upload(self, filename, blob):
        self.log(f"FTPS upload starting: {filename} ({len(blob)} bytes)")
        ftp = None
        try:
            ftp = self._prepare_ftps()
            got_final_reply = self._manual_ftps_upload(ftp, filename, blob)

            if not got_final_reply:
                if not self._remote_file_exists(filename):
                    raise TimeoutError("Upload bytes were sent, but final FTP reply timed out and file verification failed")

            self.log(f"FTPS upload complete: {filename}")
        finally:
            if ftp is not None:
                try:
                    ftp.quit()
                except Exception:
                    try:
                        ftp.close()
                    except Exception:
                        pass

    def find_gcode_inside_3mf(self, blob):
        with zipfile.ZipFile(io.BytesIO(blob), "r") as zf:
            names = [n for n in zf.namelist() if not n.endswith("/")]
            gcode_files = [n for n in names if n.lower().endswith(".gcode")]

            if not gcode_files:
                raise ValueError("No .gcode file found inside this 3MF archive.")

            def plate_sort_key(path):
                m = re.search(r"plate_(\d+)\.gcode$", path, re.IGNORECASE)
                return int(m.group(1)) if m else 999999

            exact_plate = sorted(
                [n for n in gcode_files if re.fullmatch(r"Metadata/plate_\d+\.gcode", n, re.IGNORECASE)],
                key=plate_sort_key,
            )
            generic_plate = sorted(
                [n for n in gcode_files if "plate_" in n.lower() and n not in exact_plate],
                key=plate_sort_key,
            )
            metadata_other = [
                n for n in gcode_files if "metadata/" in n.lower() and n not in exact_plate and n not in generic_plate
            ]
            everything_else = [n for n in gcode_files if n not in exact_plate and n not in generic_plate and n not in metadata_other]

            ordered = exact_plate + generic_plate + metadata_other + everything_else
            chosen = ordered[0]
            self.log(f"Chosen gcode entry: {chosen}")
            return chosen

    def read_gcode_text_from_3mf(self, blob):
        entry_name = self.find_gcode_inside_3mf(blob)
        with zipfile.ZipFile(io.BytesIO(blob), "r") as zf:
            raw = zf.read(entry_name)
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            text = raw.decode("utf-8", errors="replace")
        return entry_name, text

    def replace_zip_entries(self, blob, replacements):
        normalized = {}
        for name, data in (replacements or {}).items():
            if isinstance(data, str):
                normalized[name] = data.encode("utf-8")
            else:
                normalized[name] = data

        out = io.BytesIO()
        with zipfile.ZipFile(io.BytesIO(blob), "r") as zin:
            with zipfile.ZipFile(out, "w") as zout:
                seen = set()
                for item in zin.infolist():
                    if item.filename in normalized:
                        data = normalized[item.filename]
                        seen.add(item.filename)
                    else:
                        data = zin.read(item.filename)

                    zi = zipfile.ZipInfo(filename=item.filename, date_time=item.date_time)
                    zi.compress_type = item.compress_type
                    zi.comment = item.comment
                    zi.extra = item.extra
                    zi.create_system = item.create_system
                    zi.external_attr = item.external_attr
                    zi.internal_attr = item.internal_attr
                    zi.flag_bits = item.flag_bits
                    zout.writestr(zi, data)

                for name, data in normalized.items():
                    if name in seen:
                        continue
                    zout.writestr(name, data)

        return out.getvalue()

    def replace_zip_entry_text(self, blob, entry_name, new_text):
        return self.replace_zip_entries(blob, {entry_name: new_text})

    def build_material_profile_label(self, material: str, brand: str) -> str:
        vendor = brand_to_vendor_label(brand)
        material_name = normalize_material_choice(material)
        return f"{vendor} {material_name} @{APP_NAME} P1S 0.4 nozzle"

    def _json_value_like(self, existing, value):
        if isinstance(existing, list):
            if len(existing) > 1:
                return [str(value) for _ in existing]
            return [str(value)]
        return str(value)

    def apply_material_profile_to_gcode_text(self, gcode_text: str, material: str, brand: str, color: str) -> str:
        material_name = normalize_material_choice(material)
        brand_name = normalize_brand_choice(brand)
        color_name = normalize_color_choice(color)
        color_hex = color_name_to_hex(color_name)
        vendor_label = brand_to_vendor_label(brand_name)
        profile_label = self.build_material_profile_label(material_name, brand_name)
        profile = material_profile_for_choice(material_name)

        text = gcode_text.replace("\r\n", "\n").replace("\r", "\n")
        replacements = [
            (r'(?im)^; filament_type = .*$' , f"; filament_type = {material_name}"),
            (r'(?im)^; filament_vendor = .*$' , f'; filament_vendor = "{vendor_label}"'),
            (r'(?im)^; default_filament_profile = .*$' , f'; default_filament_profile = "{profile_label}"'),
            (r'(?im)^; filament_settings_id = .*$' , f'; filament_settings_id = "{profile_label}"'),
        ]

        if color_hex:
            replacements.extend(
                [
                    (r'(?im)^; filament_colour = .*$' , f"; filament_colour = {color_hex}"),
                    (r'(?im)^; filament_multi_colour = .*$' , f"; filament_multi_colour = {color_hex}"),
                    (r'(?im)^; default_filament_colour = .*$' , f'; default_filament_colour = "{color_hex}"'),
                ]
            )

        if profile:
            replacements.extend(
                [
                    (r'(?im)^; nozzle_temperature = .*$' , f"; nozzle_temperature = {profile['nozzle']}"),
                    (r'(?im)^; nozzle_temperature_initial_layer = .*$' , f"; nozzle_temperature_initial_layer = {profile['nozzle']}"),
                    (r'(?im)^; nozzle_temperature_range_low = .*$' , f"; nozzle_temperature_range_low = {profile['range_low']}"),
                    (r'(?im)^; nozzle_temperature_range_high = .*$' , f"; nozzle_temperature_range_high = {profile['range_high']}"),
                ]
            )

        for pattern, replacement in replacements:
            text = re.sub(pattern, replacement, text)

        if profile:
            marker_prefix = APP_NAME.upper()
            start_marker = f"; {marker_prefix} MATERIAL OVERRIDE START"
            end_marker = f"; {marker_prefix} MATERIAL OVERRIDE END"
            override_lines = [
                start_marker,
                f"; Selected material: {material_name} / {vendor_label} / {color_name}",
                f"M140 S{profile['bed']}",
                f"M190 S{profile['bed']}",
                f"M104 S{profile['nozzle']}",
                f"M109 S{profile['nozzle']}",
                end_marker,
            ]
            override_block = "\n".join(override_lines)
            if start_marker in text and end_marker in text:
                text = re.sub(
                    rf"(?s){re.escape(start_marker)}.*?{re.escape(end_marker)}",
                    override_block,
                    text,
                    count=1,
                )
            else:
                executable_marker = re.search(r"(?im)^; EXECUTABLE_BLOCK_START\s*$", text)
                if executable_marker:
                    insert_at = executable_marker.end()
                    text = text[:insert_at] + "\n" + override_block + text[insert_at:]
                else:
                    text = override_block + "\n\n" + text

        return text

    def apply_material_metadata_to_3mf(self, blob: bytes, material: str, brand: str, color: str) -> bytes:
        material_name = normalize_material_choice(material)
        brand_name = normalize_brand_choice(brand)
        color_name = normalize_color_choice(color)
        color_hex = color_name_to_hex(color_name)
        vendor_label = brand_to_vendor_label(brand_name)
        profile_label = self.build_material_profile_label(material_name, brand_name)
        profile = material_profile_for_choice(material_name)

        replacements = {}

        try:
            with zipfile.ZipFile(io.BytesIO(blob), "r") as zf:
                names = {name.lower(): name for name in zf.namelist()}

                project_name = names.get("metadata/project_settings.config")
                if project_name:
                    project_text = zf.read(project_name).decode("utf-8", errors="replace")
                    project_data = json.loads(project_text)
                    project_data["filament_type"] = self._json_value_like(project_data.get("filament_type", []), material_name)
                    project_data["filament_vendor"] = self._json_value_like(project_data.get("filament_vendor", []), vendor_label)
                    project_data["default_filament_profile"] = self._json_value_like(project_data.get("default_filament_profile", []), profile_label)
                    project_data["filament_settings_id"] = self._json_value_like(project_data.get("filament_settings_id", []), profile_label)

                    if color_hex:
                        for key in ("filament_colour", "filament_multi_colour", "default_filament_colour", "extruder_colour"):
                            project_data[key] = self._json_value_like(project_data.get(key, []), color_hex)

                    if profile:
                        for key in ("nozzle_temperature", "nozzle_temperature_initial_layer"):
                            project_data[key] = self._json_value_like(project_data.get(key, []), profile["nozzle"])
                        project_data["nozzle_temperature_range_low"] = self._json_value_like(
                            project_data.get("nozzle_temperature_range_low", ""),
                            profile["range_low"],
                        )
                        project_data["nozzle_temperature_range_high"] = self._json_value_like(
                            project_data.get("nozzle_temperature_range_high", ""),
                            profile["range_high"],
                        )

                    replacements[project_name] = json.dumps(project_data, indent=4)

                slice_name = names.get("metadata/slice_info.config")
                if slice_name:
                    root = ET.fromstring(zf.read(slice_name).decode("utf-8", errors="replace"))
                    for filament_node in root.findall(".//filament"):
                        filament_node.set("type", material_name)
                        if color_hex:
                            filament_node.set("color", color_hex)
                    replacements[slice_name] = ET.tostring(root, encoding="utf-8", xml_declaration=True).decode("utf-8")

                plate_json_name = names.get("metadata/plate_1.json")
                if plate_json_name:
                    plate_data = json.loads(zf.read(plate_json_name).decode("utf-8", errors="replace"))
                    if color_hex:
                        plate_data["filament_colors"] = [color_hex]
                    replacements[plate_json_name] = json.dumps(plate_data, separators=(",", ":"))
        except Exception as metadata_err:
            self.log(f"Material metadata rewrite skipped: {metadata_err}")
            return blob

        if not replacements:
            return blob
        return self.replace_zip_entries(blob, replacements)

    def load_flowq_eject_gcode(self):
        path = Path(FLOWQ_EJECT_GCODE_PATH)
        if not path.exists():
            raise FileNotFoundError(f"Eject file not found: {path.resolve()}")
        text = path.read_text(encoding="utf-8", errors="replace")
        return text.replace("\r\n", "\n").replace("\r", "\n").strip()

    def strip_nozzle_load_line_block(self, gcode_text):
        text = gcode_text.replace("\r\n", "\n").replace("\r", "\n")
        lines = text.split("\n")

        start_idx = None
        end_idx = None

        for i, line in enumerate(lines):
            if ";===== nozzle load line" in line.lower():
                start_idx = i
                break

        if start_idx is None:
            return text

        for i in range(start_idx + 1, len(lines)):
            if ";===== for textured pei plate" in lines[i].lower():
                end_idx = i
                break

        if end_idx is None:
            return text

        new_lines = lines[:start_idx] + lines[end_idx:]
        return "\n".join(new_lines).strip("\n")

    def extract_flowq_segments(self, gcode_text):
        text = gcode_text.replace("\r\n", "\n").replace("\r", "\n")
        lines = text.split("\n")

        executable_start = None
        machine_end_start = None
        executable_end = None

        for i, line in enumerate(lines):
            if line.strip().upper() == "; EXECUTABLE_BLOCK_START":
                executable_start = i
                break

        for i, line in enumerate(lines):
            if line.strip().upper() == "; MACHINE_END_GCODE_START":
                machine_end_start = i
                break

        for i, line in enumerate(lines):
            if line.strip().upper() == "; EXECUTABLE_BLOCK_END":
                executable_end = i
                break

        if executable_start is None:
            raise ValueError("Could not find ; EXECUTABLE_BLOCK_START in the source gcode.")
        if machine_end_start is None:
            raise ValueError("Could not find ; MACHINE_END_GCODE_START in the source gcode.")
        if executable_end is None:
            executable_end = len(lines) - 1
        if not (executable_start < machine_end_start <= executable_end):
            raise ValueError("Found invalid executable/machine-end marker positions.")

        preamble = "\n".join(lines[:executable_start]).strip("\n")
        executable_repeat = "\n".join(lines[executable_start:machine_end_start]).strip("\n")
        footer = "\n".join(lines[machine_end_start:executable_end + 1]).strip("\n")
        executable_repeat = self.strip_nozzle_load_line_block(executable_repeat)
        return preamble, executable_repeat, footer

    def detect_minutes_per_copy_from_gcode(self, gcode_text: str) -> int:
        text = gcode_text.replace("\r\n", "\n").replace("\r", "\n")
        label_priority = [
            "total estimated time",
            "estimated printing time",
            "estimated time",
            "printing time",
            "model printing time",
            "print time",
        ]

        for label in label_priority:
            pattern = rf"{re.escape(label)}\s*[:=]\s*([^;\r\n]+)"
            for match in re.finditer(pattern, text, re.IGNORECASE):
                total_seconds = self.parse_duration_to_seconds(match.group(1))
                if total_seconds > 0:
                    return max(1, int(round(total_seconds / 60)))
        return 0

    def parse_duration_to_seconds(self, value: str) -> int:
        total_seconds = 0.0
        parts = re.findall(
            r"(\d+(?:[.,]\d+)?)\s*(hours?|hrs?|hr|h|minutes?|mins?|min|m|seconds?|secs?|sec|s)\b",
            str(value or ""),
            re.IGNORECASE,
        )

        for amount_text, unit_text in parts:
            amount = float(amount_text.replace(",", "."))
            unit = unit_text.lower()
            if unit.startswith("h"):
                total_seconds += amount * 3600
            elif unit.startswith("m"):
                total_seconds += amount * 60
            elif unit.startswith("s"):
                total_seconds += amount

        return int(round(total_seconds))

    def detect_minutes_per_copy_from_slice_info(self, blob: bytes) -> int:
        with zipfile.ZipFile(io.BytesIO(blob), "r") as zf:
            candidates = sorted(
                [n for n in zf.namelist() if n.lower().endswith("slice_info.config")],
                key=lambda n: (0 if n.lower() == "metadata/slice_info.config" else 1, n.lower()),
            )
            if not candidates:
                return 0

            raw = zf.read(candidates[0])

        root = ET.fromstring(raw.decode("utf-8", errors="replace"))
        for node in root.findall(".//metadata[@key='prediction']"):
            value_text = str(node.attrib.get("value", "")).strip().replace(",", ".")
            try:
                total_seconds = float(value_text)
            except ValueError:
                continue
            if total_seconds > 0:
                return max(1, int(round(total_seconds / 60)))

        return 0

    def detect_minutes_per_copy_from_3mf(self, blob: bytes) -> int:
        try:
            minutes = self.detect_minutes_per_copy_from_slice_info(blob)
            if minutes > 0:
                return minutes
        except Exception:
            pass

        _, source_gcode_text = self.read_gcode_text_from_3mf(blob)
        return self.detect_minutes_per_copy_from_gcode(source_gcode_text)

    def build_flowq_gcode_text(self, source_gcode_text, copies, eject_gcode_text, auto_ejection_mode):
        if copies < 1:
            raise ValueError("Copies must be at least 1.")

        preamble, repeat_block, footer = self.extract_flowq_segments(source_gcode_text)

        preamble = preamble.strip()
        repeat_block = repeat_block.strip()
        footer = footer.strip()
        eject_gcode_text = eject_gcode_text.strip()

        if not repeat_block:
            raise ValueError("Repeat block is empty after extraction.")
        if not footer:
            raise ValueError("Footer is empty after extraction.")

        needs_eject = auto_ejection_mode in {
            AUTO_EJECT_BETWEEN,
            AUTO_EJECT_FINAL,
            AUTO_EJECT_ALWAYS,
        }
        if needs_eject and not eject_gcode_text:
            raise ValueError("Selected auto-eject mode needs eject gcode, but eject file is empty.")

        parts = []

        if preamble:
            parts.append(preamble)

        for i in range(copies):
            copy_number = i + 1
            is_last = copy_number == copies

            parts.append(f"; === {APP_NAME.upper()} COPY {copy_number}/{copies} START ===")
            parts.append(repeat_block)

            should_insert_eject = False
            if auto_ejection_mode == AUTO_EJECT_BETWEEN:
                should_insert_eject = not is_last
            elif auto_ejection_mode == AUTO_EJECT_FINAL:
                should_insert_eject = is_last
            elif auto_ejection_mode == AUTO_EJECT_ALWAYS:
                should_insert_eject = True

            if should_insert_eject:
                parts.append(f"; === {APP_NAME.upper()} EJECT AFTER COPY {copy_number}/{copies} ===")
                parts.append(eject_gcode_text)
                parts.append("G90")
                parts.append("G21")
                parts.append("M83")
                parts.append("G92 E0")

        parts.append(f"; === {APP_NAME.upper()} FINAL MACHINE END ===")
        parts.append(footer)

        combined = "\n\n".join(p for p in parts if p).strip() + "\n"
        return combined

    def build_flowq_3mf(
        self,
        original_filename,
        original_blob,
        copies,
        auto_ejection_mode,
        output_filename=None,
        material="Generic",
        brand="Generic",
        color="Custom",
    ):
        entry_name, source_gcode_text = self.read_gcode_text_from_3mf(original_blob)
        source_gcode_text = self.apply_material_profile_to_gcode_text(source_gcode_text, material, brand, color)

        eject_gcode_text = ""
        if auto_ejection_mode in {AUTO_EJECT_BETWEEN, AUTO_EJECT_FINAL, AUTO_EJECT_ALWAYS}:
            eject_gcode_text = self.load_flowq_eject_gcode()

        new_gcode_text = self.build_flowq_gcode_text(source_gcode_text, copies, eject_gcode_text, auto_ejection_mode)
        replacements = {entry_name: new_gcode_text}
        gcode_md5_entry = f"{entry_name}.md5"
        replacements[gcode_md5_entry] = hashlib.md5(new_gcode_text.encode("utf-8")).hexdigest().upper()
        new_blob = self.replace_zip_entries(original_blob, replacements)
        new_blob = self.apply_material_metadata_to_3mf(new_blob, material, brand, color)

        if output_filename:
            new_filename = sanitize_filename(output_filename)
        else:
            base = os.path.basename(original_filename)
            base = re.sub(r"(\.gcode)?\.3mf$", "", base, flags=re.IGNORECASE)
            new_filename = f"{base}_{APP_FILE_TAG}_{copies}x.gcode.3mf"

        if FLOWQ_SAVE_GENERATED_LOCAL:
            out_path = FLOWQ_OUTPUT_DIR / new_filename
            out_path.write_bytes(new_blob)
            self.log(f"Generated local file: {out_path.resolve()}")

        return new_filename, new_blob

    def _clear_last_command_reply(self):
        with self.last_command_reply_lock:
            self.last_command_reply = None

    def _get_last_command_reply(self):
        with self.last_command_reply_lock:
            return dict(self.last_command_reply) if self.last_command_reply else None

    def _build_project_payload(self, safe_name: str, gcode_entry: str, opts: dict, url_value: str):
        return {
            "print": {
                "sequence_id": self.next_seq(),
                "command": "project_file",
                "param": gcode_entry,
                "subtask_name": safe_name,
                "plate_idx": 0,
                "url": url_value,
                "timelapse": bool(opts["timelapse"]),
                "bed_type": "auto",
                "bed_leveling": bool(opts["bed_levelling"]),
                "bed_levelling": bool(opts["bed_levelling"]),
                "flow_cali": bool(opts["flow_cali"]),
                "vibration_cali": bool(opts["vibration_cali"]),
                "layer_inspect": bool(opts["layer_inspect"]),
                "use_ams": bool(opts["use_ams"]),
                "ams_mapping": [] if not opts["use_ams"] else [0, -1],
                "profile_id": "0",
                "project_id": "0",
                "subtask_id": "0",
                "task_id": "0",
            }
        }

    def _wait_for_project_reply_or_start(self, timeout_seconds: int = 12):
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            if self.looks_like_start_confirmed():
                return True, None

            reply = self._get_last_command_reply()
            if reply and reply.get("command") == "project_file":
                result = str(reply.get("result", "")).lower()
                reason = str(reply.get("reason", "")).strip()
                if result == "success":
                    return True, None
                if result and result != "success":
                    return False, reason or "project_file rejected"

            try:
                self.refresh()
            except Exception:
                pass
            time.sleep(1.0)

        reply = self._get_last_command_reply()
        if reply and reply.get("command") == "project_file":
            result = str(reply.get("result", "")).lower()
            reason = str(reply.get("reason", "")).strip()
            if result == "success":
                return True, None
            if result and result != "success":
                return False, reason or "project_file rejected"

        if self.looks_like_start_confirmed():
            return True, None

        return False, "No start confirmation from printer"

    def _send_project_file_with_fallbacks(self, safe_name: str, gcode_entry: str, opts: dict):
        url_variants = [
            f"file:///sdcard/cache/{safe_name}",
            f"file:///mnt/sdcard/cache/{safe_name}",
            f"ftp:///cache/{safe_name}",
        ]

        last_error = "No start confirmation from printer"

        for url_value in url_variants:
            self._clear_last_command_reply()
            payload = self._build_project_payload(safe_name, gcode_entry, opts, url_value)
            self.log(f"Trying project_file start with url={url_value}")
            self.publish(payload)

            ok, err = self._wait_for_project_reply_or_start(timeout_seconds=12)
            if ok:
                self.log(f"Start confirmed using url={url_value}")
                return

            last_error = err or last_error
            self.log(f"Start variant failed: {url_value} -> {last_error}")

        raise RuntimeError(last_error)

    def upload_and_start_print(self, filename, blob, opts):
        safe_name = sanitize_filename(filename)
        self.log(f"Preparing upload for {safe_name}")

        gcode_entry = self.find_gcode_inside_3mf(blob)
        self.ftps_upload(safe_name, blob)

        if not self._remote_file_exists(safe_name):
            raise RuntimeError(f"Uploaded file not visible on printer: {safe_name}")

        self.log(f"Upload verified on printer: {safe_name}")
        self._send_project_file_with_fallbacks(safe_name, gcode_entry, opts)
        self.queue_last_started_filename = safe_name
        self.persist_runtime_state()
        threading.Timer(1.0, self.refresh).start()
        return safe_name


class PrinterFarm:
    def __init__(self):
        self.runtimes: dict[str, Bambu] = {}
        self.order: list[str] = []
        self.refresh_from_db(connect_new=False)

    def refresh_from_db(self, connect_new=True):
        configs = printer_list()
        seen_ids = []

        for config in configs:
            printer_id = config["id"]
            seen_ids.append(printer_id)
            runtime = self.runtimes.get(printer_id)
            if runtime is None:
                runtime = Bambu(config)
                self.runtimes[printer_id] = runtime
                if connect_new:
                    runtime.connect()
            else:
                runtime.sync_shared_flags()

        self.order = seen_ids

        for printer_id in list(self.runtimes.keys()):
            if printer_id not in seen_ids:
                del self.runtimes[printer_id]

        selected_id = selected_machine_printer_id()
        if not selected_id and self.order:
            set_selected_machine_printer_id(self.order[0])

    def connect_all(self):
        for runtime in self.all_runtimes():
            if runtime.client is None:
                runtime.connect()

    def all_runtimes(self):
        return [self.runtimes[printer_id] for printer_id in self.order if printer_id in self.runtimes]

    def get_runtime(self, printer_id: str | None):
        if printer_id and printer_id in self.runtimes:
            return self.runtimes[printer_id]
        return None

    def primary_runtime(self):
        return self.get_runtime(self.order[0]) if self.order else None

    def selected_runtime(self):
        runtime = self.get_runtime(selected_machine_printer_id())
        return runtime or self.primary_runtime()

    def sync_shared_flags(self):
        for runtime in self.all_runtimes():
            runtime.sync_shared_flags()

    def set_autorun_enabled(self, enabled: bool):
        state_set("queue_autorun_enabled", "1" if enabled else "0")
        for runtime in self.all_runtimes():
            runtime.queue_autorun_enabled = enabled

    def set_manual_swap_waiting(self, item_id: str):
        state_set_many({"manual_swap_active": "1", "manual_swap_item_id": item_id})
        for runtime in self.all_runtimes():
            runtime.manual_swap_active = True
            runtime.manual_swap_item_id = item_id

    def clear_manual_swap_waiting(self):
        state_set_many({"manual_swap_active": "0", "manual_swap_item_id": ""})
        for runtime in self.all_runtimes():
            runtime.manual_swap_active = False
            runtime.manual_swap_item_id = None

    def manual_swap_active(self) -> bool:
        return state_bool_get("manual_swap_active", False)

    def manual_swap_item_id(self):
        return state_get("manual_swap_item_id", "").strip() or None

    def active_runtime_for_item(self, item_id: str):
        for runtime in self.all_runtimes():
            active_item = runtime.effective_current_queue_item()
            if active_item and active_item.get("id") == item_id:
                return runtime
        return None

    def active_item_ids(self):
        ids = set()
        for runtime in self.all_runtimes():
            active_item = runtime.effective_current_queue_item()
            if active_item:
                ids.add(active_item["id"])
        return ids

    def current_item_id_for_selected_runtime(self):
        runtime = self.selected_runtime()
        if not runtime:
            return None
        active_item = runtime.effective_current_queue_item()
        return active_item["id"] if active_item else runtime.queue_current_item_id

    def logs_drain(self):
        items = []
        for runtime in self.all_runtimes():
            items.extend(runtime.logs)
            runtime.logs.clear()
        return items

    def printer_status_payloads(self):
        payloads = []
        for runtime in self.all_runtimes():
            active_item = runtime.effective_current_queue_item()
            payloads.append(
                {
                    "id": runtime.printer_id,
                    "name": runtime.printer_name,
                    "model": runtime.printer_model,
                    "ip": runtime.printer_ip,
                    "camera_url": runtime.camera_url,
                    "connected": runtime.connected,
                    "gcode_state": runtime.status.get("gcode_state", "Disconnected"),
                    "mc_percent": int(runtime.status.get("mc_percent", 0) or 0),
                    "remaining_time_seconds": int(runtime.status.get("remaining_time", 0) or 0),
                    "remaining_time_str": runtime.format_time(runtime.status.get("remaining_time", 0)),
                    "layer_num": int(runtime.status.get("layer_num", 0) or 0),
                    "total_layer_num": int(runtime.status.get("total_layer_num", 0) or 0),
                    "nozzle_temper": int(runtime.status.get("nozzle_temper", 0) or 0),
                    "nozzle_target_temper": int(runtime.status.get("nozzle_target_temper", 0) or 0),
                    "bed_temper": int(runtime.status.get("bed_temper", 0) or 0),
                    "bed_target_temper": int(runtime.status.get("bed_target_temper", 0) or 0),
                    "spd_lvl": runtime.status.get("spd_lvl", "-"),
                    "gcode_file": runtime.status.get("gcode_file", "-"),
                    "queue_current_item_id": active_item["id"] if active_item else runtime.queue_current_item_id,
                }
            )
        return payloads


UTILITY_RUNTIME = None


def queue_first_queued_item():
    with db_conn() as conn:
        row = conn.execute(
            "SELECT * FROM queue_items WHERE status = 'queued' ORDER BY position ASC, created_at ASC LIMIT 1"
        ).fetchone()
    return dict(row) if row else None


def queue_first_eligible_for_printer(printer_id: str):
    for item in queue_list():
        if str(item.get("status", "")).lower() != "queued":
            continue

        item_type = str(item.get("item_type", QUEUE_ITEM_TYPE_PRINT) or QUEUE_ITEM_TYPE_PRINT)
        if item_type == QUEUE_ITEM_TYPE_FILAMENT_SWAP:
            return None

        if queue_item_matches_printer(item, printer_id):
            return item

    return None


def all_printers_idle(printer_farm: PrinterFarm):
    for runtime in printer_farm.all_runtimes():
        if runtime.queue_current_item_id or runtime.queue_launch_busy or runtime.is_runningish():
            return False
    return True


def utility_printer():
    runtime = printer_farm.primary_runtime() if "printer_farm" in globals() else None
    if runtime:
        return runtime

    global UTILITY_RUNTIME
    if UTILITY_RUNTIME is None:
        UTILITY_RUNTIME = Bambu(
            {
                "id": "__utility__",
                "name": "Queue Utility",
                "model": DEFAULT_PRINTER_MODEL,
                "ip": "",
                "access_code": "",
                "serial": "",
                "camera_url": "",
            }
        )
    return UTILITY_RUNTIME


def runtime_for_request(prefer_request_json=False):
    printer_id = ""
    if prefer_request_json and request.is_json:
        printer_id = str((request.json or {}).get("printer_id", "") or "").strip()
    if not printer_id:
        printer_id = str(request.args.get("printer_id", "") or "").strip()
    if not printer_id and request.method == "POST":
        printer_id = str(request.form.get("printer_id", "") or "").strip()

    runtime = printer_farm.get_runtime(printer_id) if printer_id else None
    return runtime or printer_farm.selected_runtime()


def active_runtime_map_by_item_id():
    mapping = {}
    for runtime in printer_farm.all_runtimes():
        active_item = runtime.effective_current_queue_item()
        if active_item:
            mapping[active_item["id"]] = runtime
    return mapping


def queue_worker():
    while True:
        time.sleep(QUEUE_POLL_SECONDS)
        printer_farm.sync_shared_flags()
        runtimes = printer_farm.all_runtimes()

        if not runtimes:
            continue

        for runtime in runtimes:
            if not runtime.connected or not runtime.status_report_received:
                continue

            with runtime.queue_worker_lock:
                try:
                    runtime.reconcile_runtime_with_printer()

                    if printer_farm.manual_swap_active():
                        waiting = queue_waiting_filament_swap()
                        if not waiting or waiting["id"] != printer_farm.manual_swap_item_id():
                            runtime.log("Manual swap state cleared because waiting task no longer exists")
                            printer_farm.clear_manual_swap_waiting()
                        continue

                    if runtime.queue_current_item_id is not None:
                        item = queue_get(runtime.queue_current_item_id)
                        if not item:
                            runtime.log("Current queue item missing from DB, clearing runtime")
                            runtime.clear_queue_runtime()
                            continue

                        if runtime.looks_like_start_confirmed():
                            if not runtime.queue_seen_running:
                                runtime.queue_seen_running = True
                                queue_update(runtime.queue_current_item_id, assigned_printer_id=runtime.printer_id)
                                queue_mark_started(runtime.queue_current_item_id)
                                runtime.persist_runtime_state()
                                runtime.apply_speed_for_current_queue_item_delayed()
                                runtime.log(f"Print confirmed started: {runtime.queue_current_item_id}")

                        if runtime.queue_seen_running:
                            runtime.maybe_enforce_current_queue_speed()

                        if not runtime.queue_seen_running:
                            if runtime.queue_start_requested_at is None:
                                runtime.queue_start_requested_at = time.time()

                            age = time.time() - runtime.queue_start_requested_at
                            if age >= START_CONFIRM_TIMEOUT_SECONDS:
                                if runtime.queue_retry_count < START_RETRY_LIMIT:
                                    runtime.queue_retry_count += 1
                                    runtime.log(
                                        f"Start not confirmed, retrying {runtime.queue_retry_count}/{START_RETRY_LIMIT}"
                                    )

                                    file_path = Path(item["file_path"])
                                    if not file_path.exists():
                                        queue_mark_error(item["id"], f"Generated file missing: {file_path}")
                                        runtime.clear_queue_runtime()
                                        continue

                                    options = parse_options_json(item["options_json"])
                                    blob = file_path.read_bytes()

                                    try:
                                        started_name = runtime.upload_and_start_print(
                                            item["generated_filename"],
                                            blob,
                                            options,
                                        )
                                        runtime.queue_last_started_filename = started_name
                                        runtime.queue_start_requested_at = time.time()
                                        runtime.persist_runtime_state()
                                    except Exception as retry_err:
                                        queue_mark_error(item["id"], f"Retry launch failed: {retry_err}")
                                        runtime.log(f"Retry launch failed: {retry_err}")
                                        runtime.clear_queue_runtime()
                                else:
                                    queue_mark_error(item["id"], "Print never confirmed start")
                                    runtime.log(f"Print never confirmed start: {item['id']}")
                                    runtime.clear_queue_runtime()
                                continue

                        if runtime.queue_seen_running and runtime.is_finished_state():
                            finished_id = runtime.queue_current_item_id
                            queue_mark_finished(finished_id)
                            runtime.log(f"Queue item finished: {finished_id}")
                            queue_delete(finished_id)
                            runtime.log(f"Removed finished queue item: {finished_id}")
                            runtime.clear_queue_runtime()
                            continue

                        if runtime.was_active_print_stopped_externally():
                            stopped_id = runtime.queue_current_item_id
                            reason = runtime.active_print_stop_reason()
                            runtime.log(f"Removing queue item {stopped_id} because {reason}")
                            queue_delete(stopped_id)
                            runtime.clear_queue_runtime()
                            continue

                except Exception as e:
                    runtime.log(f"Queue worker error: {e}")
                    if runtime.queue_current_item_id:
                        queue_mark_error(runtime.queue_current_item_id, f"Queue worker error: {e}")
                        runtime.clear_queue_runtime()
                    printer_farm.clear_manual_swap_waiting()

        autorun_enabled = state_bool_get("queue_autorun_enabled", True)
        if not autorun_enabled or printer_farm.manual_swap_active():
            continue

        first_queued_item = queue_first_queued_item()
        if (
            first_queued_item
            and str(first_queued_item.get("item_type", QUEUE_ITEM_TYPE_PRINT) or QUEUE_ITEM_TYPE_PRINT)
            == QUEUE_ITEM_TYPE_FILAMENT_SWAP
        ):
            if all_printers_idle(printer_farm):
                queue_update(first_queued_item["id"], status="waiting", started_at=now_iso(), finished_at="", last_error="")
                printer_farm.set_manual_swap_waiting(first_queued_item["id"])
                for runtime in runtimes:
                    runtime.log(f"Filament swap waiting: {first_queued_item['id']}")
            continue

        for runtime in runtimes:
            if not runtime.connected or not runtime.status_report_received:
                continue
            if runtime.queue_launch_busy or runtime.queue_current_item_id is not None or runtime.is_runningish():
                continue

            item = queue_first_eligible_for_printer(runtime.printer_id)
            if not item:
                continue

            file_path = Path(item["file_path"])
            if not file_path.exists():
                queue_mark_error(item["id"], f"Generated file missing: {file_path}")
                runtime.log(f"Queue item missing file: {file_path}")
                continue

            options = parse_options_json(item["options_json"])
            blob = file_path.read_bytes()

            runtime.queue_launch_busy = True
            runtime.queue_current_item_id = item["id"]
            runtime.queue_seen_running = False
            runtime.queue_start_requested_at = time.time()
            runtime.queue_retry_count = 0
            runtime.queue_last_started_filename = item["generated_filename"]
            runtime.persist_runtime_state()

            queue_update(item["id"], assigned_printer_id=runtime.printer_id)
            queue_mark_starting(item["id"])

            try:
                started_name = runtime.upload_and_start_print(
                    item["generated_filename"],
                    blob,
                    options,
                )
                runtime.queue_last_started_filename = started_name
                runtime.persist_runtime_state()
                runtime.log(f"Actual printer start command sent for queue item: {item['name']} -> {started_name}")
            except Exception as launch_err:
                queue_mark_error(item["id"], f"Launch failed: {launch_err}")
                runtime.log(f"Launch failed for {item['name']}: {launch_err}")
                runtime.clear_queue_runtime()
            finally:
                runtime.queue_launch_busy = False


init_db()
queue_reset_stale_starting_items()
printer_farm = PrinterFarm()
threading.Thread(target=queue_worker, daemon=True).start()


@app.route("/")
def index():
    selected_runtime = printer_farm.selected_runtime()
    return render_template_string(
        HTML,
        app_name=APP_NAME,
        app_tagline=APP_TAGLINE,
        ip=selected_runtime.printer_ip if selected_runtime else "-",
        camera_enabled=bool(selected_runtime.camera_url) if selected_runtime else False,
        camera_url=selected_runtime.camera_url if selected_runtime else "",
        flowq_default_copies=FLOWQ_DEFAULT_COPIES,
        flowq_eject_name=FLOWQ_EJECT_GCODE_PATH.name,
        flowq_output_dir=str(FLOWQ_OUTPUT_DIR),
        material_options=MATERIAL_OPTIONS,
        brand_options=BRAND_OPTIONS,
        color_options=COLOR_OPTIONS,
        auto_ejection_options=AUTO_EJECTION_OPTIONS,
        preview_options=PREVIEW_OPTIONS,
        printer_options=printer_choice_options(),
        initial_selected_printer_id=selected_machine_printer_id() or "",
    )


@app.route("/api/status")
def api_status():
    runtime = runtime_for_request()
    s = dict(runtime.status) if runtime else {}
    effective_current_item = runtime.effective_current_queue_item() if runtime else None
    s["printer_id"] = runtime.printer_id if runtime else ""
    s["printer_name"] = runtime.printer_name if runtime else ""
    s["camera_url"] = runtime.camera_url if runtime else ""
    s["camera_enabled"] = bool(runtime.camera_url) if runtime else False
    s["connected"] = runtime.connected if runtime else False
    s["remaining_time_seconds"] = int(s.get("remaining_time", 0) or 0)
    s["remaining_time_str"] = runtime.format_time(s.get("remaining_time", 0)) if runtime else "-"
    s["queue_autorun_enabled"] = state_bool_get("queue_autorun_enabled", True)
    s["queue_current_item_id"] = effective_current_item["id"] if effective_current_item else (runtime.queue_current_item_id if runtime else None)
    s["queue_seen_running"] = runtime.queue_seen_running if runtime else False
    s["queue_last_started_filename"] = runtime.queue_last_started_filename if runtime else ""
    s["manual_swap_active"] = printer_farm.manual_swap_active()
    s["manual_swap_item_id"] = printer_farm.manual_swap_item_id()
    s["selected_printer_id"] = runtime.printer_id if runtime else ""
    s["printers"] = printer_farm.printer_status_payloads()

    s["eject_test_active"] = False
    s["eject_total_cycles"] = "-"
    s["eject_detected_file"] = "-"
    s["eject_elapsed_str"] = "-"
    s["eject_startup_elapsed_str"] = "-"
    s["eject_startup_total_str"] = "-"
    s["eject_in_startup"] = False
    s["eject_estimated_cycle_str"] = "-"

    return jsonify(s)


@app.route("/api/preview/<item_id>")
def api_preview(item_id):
    item = queue_get(item_id)
    if not item:
        return ("Not found", 404)

    preview_path = str(item.get("preview_path", "") or "").strip()
    if not preview_path:
        return ("No preview", 404)

    path = Path(preview_path)
    if not path.exists():
        return ("Preview missing", 404)

    return send_file(path, mimetype="image/png", max_age=0)


@app.route("/api/queue")
def api_queue():
    items = queue_list()
    active_runtimes = active_runtime_map_by_item_id()
    current_ids = set(active_runtimes.keys())
    current_id = printer_farm.current_item_id_for_selected_runtime()
    manual_swap_id = printer_farm.manual_swap_item_id()
    out = []

    for item in items:
        d = refresh_queue_item_timing_from_file(item)
        active_runtime = active_runtimes.get(d["id"])
        d["is_current"] = d["id"] in current_ids or d["id"] == (manual_swap_id or "")
        d["estimated_seconds_per_copy"] = int(d.get("estimated_seconds_per_copy", 0) or 0)
        d["estimated_total_seconds"] = int(d.get("estimated_total_seconds", 0) or 0)
        d["actual_total_seconds"] = int(d.get("actual_total_seconds", 0) or 0)
        d["live_remaining_time_seconds"] = int(active_runtime.status.get("remaining_time", 0) or 0) if active_runtime else None
        d["current_printer_id"] = active_runtime.printer_id if active_runtime else queue_assigned_printer_id(d)
        d["current_printer_name"] = active_runtime.printer_name if active_runtime else (
            printer_name_by_id(queue_assigned_printer_id(d)) if queue_assigned_printer_id(d) else ""
        )
        d["printer"] = active_runtime.printer_name if active_runtime else queue_item_printer_label(d)
        d["repetitions_label"] = build_repetitions_label(d, d["is_current"], active_runtime.status if active_runtime else {})

        preview_path = str(d.get("preview_path", "") or "").strip()
        d["preview_url"] = f"/api/preview/{d['id']}" if preview_path and Path(preview_path).exists() else ""

        opts = parse_options_json(d.get("options_json", "{}"))

        d["speed_level"] = normalize_speed_level(opts.get("speed_level", SPEED_STANDARD))
        d["speed_label"] = speed_label(d["speed_level"])

        out.append(d)

    return jsonify(
        {
            "items": out,
            "autorun_enabled": state_bool_get("queue_autorun_enabled", True),
            "current_item_id": current_id,
            "active_filament_swap_id": manual_swap_id,
            "speed_options": [{"value": k, "label": v} for k, v in SPEED_OPTIONS.items()],
            "printer_options": printer_choice_options(),
        }
    )


@app.route("/api/queue/set_speed_all", methods=["POST"])
def api_queue_set_speed_all():
    try:
        level = normalize_speed_level(request.json.get("speed_level", SPEED_STANDARD))
        updated = queue_set_speed_for_all_prints(level)
        applied_now = 0

        for runtime in printer_farm.all_runtimes():
            current_item = runtime.effective_current_queue_item()
            if not current_item or str(current_item.get("item_type", QUEUE_ITEM_TYPE_PRINT)) != QUEUE_ITEM_TYPE_PRINT:
                continue
            try:
                runtime.speed(level)
                applied_now += 1
                runtime.log(f"Applied speed {speed_label(level)} to current print")
            except Exception as e:
                runtime.log(f"Could not apply live speed change: {e}")

        return jsonify(
            ok=True,
            message=f"Set speed to {speed_label(level)} for {updated} print(s)" + (
                f" and updated {applied_now} active print(s)" if applied_now else ""
            ),
            speed_level=level,
            speed_label=speed_label(level),
        )
    except Exception as e:
        return jsonify(ok=False, message=str(e)), 500


@app.route("/api/queue/delete", methods=["POST"])
def api_queue_delete():
    item_id = str(request.json.get("id", "")).strip()
    if not item_id:
        return jsonify(ok=False, message="Missing queue item id"), 400

    active_ids = printer_farm.active_item_ids()

    item = next((x for x in queue_list() if x["id"] == item_id), None)
    if not item:
        return jsonify(ok=False, message="Queue item not found"), 404

    status = str(item.get("status", "")).lower()
    if item_id in active_ids or item_id == printer_farm.manual_swap_item_id() or status in {"printing", "starting", "waiting"}:
        return jsonify(ok=False, message="Cannot delete an item while it is active"), 400

    queue_delete(item_id)
    return jsonify(ok=True, message="Queue item removed")


@app.route("/api/queue/move", methods=["POST"])
def api_queue_move():
    item_id = str(request.json.get("id", "")).strip()
    direction = str(request.json.get("direction", "")).strip().lower()
    if not item_id or direction not in {"up", "down"}:
        return jsonify(ok=False, message="Invalid move request"), 400

    active_ids = printer_farm.active_item_ids()

    item = next((x for x in queue_list() if x["id"] == item_id), None)
    if not item:
        return jsonify(ok=False, message="Queue item not found"), 404

    status = str(item.get("status", "")).lower()
    if item_id in active_ids or item_id == printer_farm.manual_swap_item_id() or status in {"printing", "starting", "waiting"}:
        return jsonify(ok=False, message="Cannot move an item while it is active"), 400

    changed = queue_reorder(item_id, direction)
    return jsonify(ok=True, message=f"Queue item moved {direction}" if changed else "Queue item did not move")


@app.route("/api/queue/autorun", methods=["POST"])
def api_queue_autorun():
    enabled = bool(request.json.get("enabled", True))
    printer_farm.set_autorun_enabled(enabled)
    return jsonify(ok=True, message=f"Queue autorun {'enabled' if enabled else 'disabled'}")


@app.route("/api/printers")
def api_printers():
    status_by_id = {item["id"]: item for item in printer_farm.printer_status_payloads()}
    out = []
    for printer in printer_list():
        payload = dict(printer)
        payload.update(status_by_id.get(printer["id"], {}))
        out.append(payload)

    return jsonify(
        {
            "items": out,
            "selected_printer_id": selected_machine_printer_id() or "",
            "printer_options": printer_choice_options(),
        }
    )


@app.route("/api/printers/add", methods=["POST"])
def api_printers_add():
    name = str(request.json.get("name", "")).strip()
    ip = str(request.json.get("ip", "")).strip()
    access_code = str(request.json.get("access_code", "")).strip()
    serial = str(request.json.get("serial", "")).strip()
    camera_url = str(request.json.get("camera_url", "")).strip()

    if not ip or not access_code or not serial:
        return jsonify(ok=False, message="IP, access code, and serial are required"), 400

    if not name:
        name = f"Printer {printer_next_position()}"

    existing_ip = printer_find_by_ip(ip)
    if existing_ip:
        return jsonify(ok=False, message=f"{existing_ip['name']} already uses that IP"), 400

    existing_serial = printer_find_by_serial(serial)
    if existing_serial:
        return jsonify(ok=False, message=f"{existing_serial['name']} already uses that serial"), 400

    printer_id = printer_insert(name=name, ip=ip, access_code=access_code, serial=serial, camera_url=camera_url)
    if not selected_machine_printer_id():
        set_selected_machine_printer_id(printer_id)
    printer_farm.refresh_from_db(connect_new=True)

    return jsonify(ok=True, message=f"Added {name}", id=printer_id)


@app.route("/api/printers/select", methods=["POST"])
def api_printers_select():
    printer_id = str(request.json.get("printer_id", "")).strip()
    if not printer_id or not printer_get(printer_id):
        return jsonify(ok=False, message="Printer not found"), 404

    set_selected_machine_printer_id(printer_id)
    return jsonify(ok=True, message=f"Selected {printer_name_by_id(printer_id)}")


@app.route("/api/queue/add_filament_swap", methods=["POST"])
def api_queue_add_filament_swap():
    after_id = str(request.json.get("after_id", "")).strip() or None
    message = str(request.json.get("message", "Please swap the filament and click continue.")).strip()
    try:
        item_id = queue_insert_filament_swap(after_id, message=message or "Please swap the filament and click continue.")
        return jsonify(ok=True, message="Filament swap pause added", id=item_id)
    except Exception as e:
        return jsonify(ok=False, message=str(e)), 400


@app.route("/api/queue/continue_filament_swap", methods=["POST"])
def api_queue_continue_filament_swap():
    requested_id = str(request.json.get("id", "")).strip() or printer_farm.manual_swap_item_id()
    if not requested_id:
        return jsonify(ok=False, message="No filament swap is waiting"), 400

    item = queue_get(requested_id)
    if not item or str(item.get("item_type")) != QUEUE_ITEM_TYPE_FILAMENT_SWAP:
        return jsonify(ok=False, message="Filament swap task not found"), 404

    if str(item.get("status", "")).lower() != "waiting":
        return jsonify(ok=False, message="That filament swap task is not waiting"), 400

    queue_mark_finished(requested_id)
    with db_conn() as conn:
        conn.execute("DELETE FROM queue_items WHERE id = ?", (requested_id,))
        queue_normalize_positions(conn)
        conn.commit()

    if printer_farm.manual_swap_item_id() == requested_id:
        printer_farm.clear_manual_swap_waiting()

    return jsonify(ok=True, message="Filament swap completed, queue will continue")


@app.route("/api/logs")
def api_logs():
    return jsonify(printer_farm.logs_drain())


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    try:
        refreshed = 0
        for runtime in printer_farm.all_runtimes():
            if not runtime.connected:
                continue
            runtime.refresh()
            refreshed += 1
        return jsonify(ok=True, message=f"Status refresh sent to {refreshed} printer(s)")
    except Exception as e:
        return jsonify(ok=False, message=str(e)), 500


@app.route("/api/pause", methods=["POST"])
def api_pause():
    try:
        runtime = runtime_for_request(prefer_request_json=True)
        if not runtime:
            return jsonify(ok=False, message="No printer selected"), 400
        runtime.pause()
        return jsonify(ok=True, message=f"Pause sent to {runtime.printer_name}")
    except Exception as e:
        return jsonify(ok=False, message=str(e)), 500


@app.route("/api/resume", methods=["POST"])
def api_resume():
    try:
        runtime = runtime_for_request(prefer_request_json=True)
        if not runtime:
            return jsonify(ok=False, message="No printer selected"), 400
        runtime.resume()
        return jsonify(ok=True, message=f"Resume sent to {runtime.printer_name}")
    except Exception as e:
        return jsonify(ok=False, message=str(e)), 500


@app.route("/api/stop", methods=["POST"])
def api_stop():
    try:
        runtime = runtime_for_request(prefer_request_json=True)
        if not runtime:
            return jsonify(ok=False, message="No printer selected"), 400
        effective_current_item = runtime.effective_current_queue_item()
        effective_current_id = effective_current_item["id"] if effective_current_item else runtime.queue_current_item_id
        runtime.stop()
        if effective_current_id:
            queue_mark_requeued(effective_current_id, "Stopped manually")
            runtime.clear_queue_runtime()
        return jsonify(ok=True, message=f"Stop sent to {runtime.printer_name}")
    except Exception as e:
        return jsonify(ok=False, message=str(e)), 500


@app.route("/api/speed", methods=["POST"])
def api_speed():
    try:
        runtime = runtime_for_request(prefer_request_json=True)
        if not runtime:
            return jsonify(ok=False, message="No printer selected"), 400
        runtime.speed(int(request.json.get("level", 2)))
        return jsonify(ok=True, message=f"Speed request sent to {runtime.printer_name}")
    except Exception as e:
        return jsonify(ok=False, message=str(e)), 500


@app.route("/api/gcode", methods=["POST"])
def api_gcode():
    try:
        runtime = runtime_for_request(prefer_request_json=True)
        if not runtime:
            return jsonify(ok=False, message="No printer selected"), 400
        gcode = str(request.json.get("gcode", "")).strip()
        if not gcode:
            return jsonify(ok=False, message="Missing G-code"), 400
        runtime.gcode(gcode)
        return jsonify(ok=True, message=f"G-code sent to {runtime.printer_name}: {gcode}")
    except Exception as e:
        return jsonify(ok=False, message=str(e)), 500


@app.route("/api/temp", methods=["POST"])
def api_temp():
    try:
        runtime = runtime_for_request(prefer_request_json=True)
        if not runtime:
            return jsonify(ok=False, message="No printer selected"), 400
        kind = request.json.get("kind")
        value = int(request.json.get("value", 0))

        if kind == "nozzle":
            if not 0 <= value <= 320:
                return jsonify(ok=False, message="Nozzle must be 0-320"), 400
            runtime.gcode(f"M104 S{value}")
            return jsonify(ok=True, message=f"Nozzle target requested: {value}°C")

        if kind == "bed":
            if not 0 <= value <= 120:
                return jsonify(ok=False, message="Bed must be 0-120"), 400
            runtime.gcode(f"M140 S{value}")
            return jsonify(ok=True, message=f"Bed target requested: {value}°C")

        return jsonify(ok=False, message="Invalid temp type"), 400
    except Exception as e:
        return jsonify(ok=False, message=str(e)), 500


@app.route("/api/detect_time", methods=["POST"])
def api_detect_time():
    f = request.files.get("file")
    if not f or not f.filename:
        return jsonify(ok=False, message="No file uploaded"), 400

    try:
        blob = f.read()
        if not blob:
            return jsonify(ok=False, message="Uploaded file is empty"), 400

        minutes = utility_printer().detect_minutes_per_copy_from_3mf(blob)
        return jsonify(ok=True, minutes_per_copy=minutes)
    except Exception as e:
        utility_printer().log(f"Time detect failed: {e}")
        return jsonify(ok=False, message=str(e), minutes_per_copy=0), 200


@app.route("/api/build_print", methods=["POST"])
def api_build_print():
    if not FLOWQ_ENABLED:
        return jsonify(ok=False, message="Print building is disabled"), 400

    f = request.files.get("file")
    if not f or not f.filename:
        return jsonify(ok=False, message="No source file uploaded"), 400

    try:
        copies = int(request.form.get("copies", FLOWQ_DEFAULT_COPIES))
    except Exception:
        return jsonify(ok=False, message="Copies must be a valid number"), 400

    if copies < 1 or copies > 50:
        return jsonify(ok=False, message="Copies must be between 1 and 50"), 400

    try:
        blob = f.read()
        if not blob:
            return jsonify(ok=False, message="Uploaded file is empty"), 400

        opts = options_from_form(request.form)
        material = normalize_material_choice(opts.get("material", request.form.get("material", "Generic")))
        color = normalize_color_choice(opts.get("color", request.form.get("color", "Black")))
        brand = normalize_brand_choice(opts.get("brand", request.form.get("brand", "Generic")))
        target_printer_id = str(request.form.get("target_printer_id", "") or "").strip()
        auto_ejection = request.form.get("auto_ejection", AUTO_EJECT_NONE)
        repetition_method = request.form.get("repetition_method", "Non-sticky")
        preview_emoji = request.form.get("preview_emoji", "⬛")
        minutes_override = int(request.form.get("minutes_per_copy", "0") or "0")
        opts["minutes_per_copy_override"] = minutes_override if minutes_override > 0 else 0
        opts["target_printer_id"] = target_printer_id

        if target_printer_id and not printer_get(target_printer_id):
            return jsonify(ok=False, message="Selected printer does not exist"), 400

        detected_minutes = 0
        try:
            detected_minutes = utility_printer().detect_minutes_per_copy_from_3mf(blob)
        except Exception as detect_err:
            utility_printer().log(f"Print time auto detect failed: {detect_err}")

        minutes_per_copy = minutes_override if minutes_override > 0 else detected_minutes
        if minutes_per_copy <= 0:
            minutes_per_copy = 120

        speed_level = normalize_speed_level(opts.get("speed_level", SPEED_STANDARD))

        item_id = str(uuid.uuid4())
        unique_generated_filename = make_unique_flowq_filename(f.filename, copies)

        new_filename, new_blob = utility_printer().build_flowq_3mf(
            f.filename,
            blob,
            copies,
            auto_ejection,
            output_filename=unique_generated_filename,
            material=material,
            brand=brand,
            color=color,
        )

        out_path = FLOWQ_OUTPUT_DIR / new_filename
        out_path.write_bytes(new_blob)

        preview_path = generate_preview_file_for_queue_item(
            item_id=item_id,
            blob=blob,
            title=Path(new_filename).stem,
            material=material,
            color=color,
            preview_emoji=preview_emoji,
        )

        estimated_seconds_per_copy = scale_estimated_seconds_for_speed(minutes_per_copy * 60, speed_level)
        estimated_total_seconds = int(estimated_seconds_per_copy * copies)

        queue_insert(
            {
                "id": item_id,
                "created_at": now_iso(),
                "updated_at": now_iso(),
                "position": queue_next_position(),
                "name": Path(new_filename).stem,
                "source_filename": f.filename,
                "generated_filename": new_filename,
                "file_path": str(out_path),
                "copies": copies,
                "repetitions_label": f"0 of {copies}" if copies > 1 else "1 of 1",
                "repetition_method": repetition_method,
                "automatic_print_ejection": auto_ejection,
                "material": material,
                "brand": brand,
                "color": color,
                "printer": printer_name_by_id(target_printer_id) if target_printer_id else FIRST_AVAILABLE_LABEL,
                "duration": format_seconds_human(estimated_total_seconds),
                "status": "queued",
                "last_error": "",
                "preview_emoji": preview_emoji,
                "preview_path": preview_path,
                "options_json": json.dumps(opts),
                "estimated_seconds_per_copy": estimated_seconds_per_copy,
                "estimated_total_seconds": estimated_total_seconds,
                "actual_total_seconds": 0,
                "started_at": "",
                "finished_at": "",
                "item_type": QUEUE_ITEM_TYPE_PRINT,
                "swap_message": "",
                "target_printer_id": target_printer_id,
                "assigned_printer_id": "",
            }
        )

        return jsonify(
            ok=True,
            message=f"Queue item added: {new_filename} ({copies} copies, {minutes_per_copy} min/copy)"
        )
    except Exception as e:
        utility_printer().log(f"Build/queue failed: {e}")
        return jsonify(ok=False, message=str(e)), 500


if __name__ == "__main__":
    requests.packages.urllib3.disable_warnings()
    printer_farm.connect_all()
    print(f"Open http://127.0.0.1:{WEB_PORT}")
    app.run(host=WEB_HOST, port=WEB_PORT, debug=False, threaded=True)
