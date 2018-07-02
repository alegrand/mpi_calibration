# MPI calibration on G5K

In this README, the command `python` is assumed to be the version 3 of Python. We also assume the user to be called `alice` and to have an account on G5K.

## Running the calibration

### Running a single calibration

This is done with the file [fabfile.py](fabfile.py).

Running a calibration on two nodes of the `taurus` cluster (which is located in Lyon):
```bash
python fabfile.py lyon alice cluster taurus
```

Running a calibration with nodes `taurus-3` and `taurus-8`:
```bash
python fabfile.py lyon alice nodes taurus-3 taurus-8
```

Running a calibration with the job 12345 in Lyon (which is assumed to be running):
```bash
python fabfile.py lyon alice jobid 12345
```

## Analyzing the calibration

See the different notebooks:

- [analysis.ipynb](analysis.ipynb)
- [analysis_deploy.ipynb](analysis_deploy.ipynb)
- [demo_LIG_day.ipynb](demo_LIG_day.ipynb)
