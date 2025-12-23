import subprocess

scripts = [
    "eye_mark_mc17_sunsilk.py",
    "eye_mark_mc18_sunsilk.py",
    "eye_mark_mc19_sunsilk.py",
    "eye_mark_mc20_sunsilk.py",
    "eye_mark_mc21_sunsilk.py",
    "eye_mark_mc22_sunsilk.py",
    "eye_mark_mc26_dove.py"
]

processes = []

for script in scripts:
    proc = subprocess.Popen(["python3", script])
    processes.append(proc)


for proc in processes:
    proc.wait()

