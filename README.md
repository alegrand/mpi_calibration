# MPI calibration on G5K

In this README, the command `python` is assumed to be the version 3 of Python. We also assume the user to be called `alice` and to have an account on G5K.

## Dependencies

See the [Pipfile](Pipfile). In particular, packages `fabric`, `colorlog` and `pyyaml` are hard dependencies for the experiment script.

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

Note that you can also use this file as a library. This is particularly useful for debugging. For instance, the
following Python code submit a G5K job, deploy an image, install a few packages and run a command. For more examples, see the file
[fabfile.py](fabfile.py).

```python
from fabfile import Job, Time
job = Job.oarsub_cluster(site='lyon',
                         username='alice',
                         clusters=['taurus'],
                         walltime=Time(minutes=15),
                         nb_nodes=2
                        )
job.kadeploy()
print(job.hostnames)
job.apt_install('python3', 'python3-dev', 'git')
job.run_frontend('touch "hello_world.txt"')
job.run_nodes('uname -r', hide_output=False)
job.oardel()
```

### Running calibrations in batch

It is often useful to run calibrations in batch.
Modify the file [runner.py](runner.py) to have the desired settings (in particular, site, username and nodes). Copy the
file [fabfile.py](fabfile.py) to the frontend node. Then, run the following:
```bash
python runner.py
```

## Analyzing the calibration

See the different notebooks:

- [analysis.ipynb](analysis.ipynb)
- [analysis_deploy.ipynb](analysis_deploy.ipynb)
- [demo_LIG_day.ipynb](demo_LIG_day.ipynb)
