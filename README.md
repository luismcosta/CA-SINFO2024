# ofac-sanctions-sinfo-2024

Demo project to present at SINFO 2024 ComplyAdvantage workshop.

## Getting started

### Install Python on Windows

- Follow the instructions at https://www.digitalocean.com/community/tutorials/install-python-windows-10

### Install Python on MacOS

- Follow the instructions at https://www.pythoncentral.io/how-to-install-python-on-mac/

### Install the project on MacOS/Linux

1. Create a new virtual environment

   ```python3 -m venv .venv```

2. Activate the virtual environment

   ```. .venv/bin/activate```

3. Install the project

   ```python3 -m pip install .```

4. Download the input sanctions file

   ```wget https://www.treasury.gov/ofac/downloads/sdn.xml```

5. Run jupyter

   ```python3 -m jupyter lab sanctions-dataset.ipynb```

### Install the project on Windows

1. Create a new virtual environment

   ```python -m venv .venv```

2. Activate the virtual environment

   ```.\.venv\bin\activate.bat```

3. Install the project

   ```python3 -m pip install .```

4. Download the input sanctions file

   ```curl https://www.treasury.gov/ofac/downloads/sdn.xml -o sdn.xml```

5. Run jupyter

   ```python3 -m jupyter lab sanctions-dataset.ipynb```