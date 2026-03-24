# LayerLoop

LayerLoop is a local web app for open-source 3D printer automation.

It gives you a queue-driven workflow for Bambu P1S printers with live status, multi-printer targeting, speed control, material overrides, repeated print generation, and manual filament-swap barriers.

## Current scope

- Local-first Flask app
- Multi-printer queue routing
- "Print on first available" or target a specific printer
- Queue-aware speed handling
- Material, brand, and color overrides baked into generated print files
- Camera support
- Manual filament swap checkpoints

## Quick start

1. Install Python 3.10+.
2. Install dependencies:

```bash
pip install flask requests paho-mqtt pillow
```

3. Start the app:

```bash
python code.py
```

4. Open `http://127.0.0.1:5000`.
5. Add your printers from the Settings menu.

## Project notes

- Printer settings and queue state are stored locally in `data/`.
- Generated print files and previews are also stored locally in `data/`.
- This repository is set up so local printer credentials and runtime data should stay untracked.
- The current printer model flow is intentionally limited to Bambu P1S.

## Publishing checklist

- Add your preferred open-source license before publishing.
- Review the UI text and defaults for your project style.
- Test against a clean `data/` directory before your first release.
- If you encounter any bugs, please dm superdaan0608 on discord.
