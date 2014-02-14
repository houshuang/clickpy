#!/bin/bash
source ~/.py3/bin/python

python videos_reduce.py mentalhealth_002.h5.repacked 1 8 /tmp/clickpy &
python videos_reduce.py mentalhealth_002.h5.repacked 2 8 /tmp/clickpy &
python videos_reduce.py mentalhealth_002.h5.repacked 3 8 /tmp/clickpy &
python videos_reduce.py mentalhealth_002.h5.repacked 4 8 /tmp/clickpy &
python videos_reduce.py mentalhealth_002.h5.repacked 5 8 /tmp/clickpy &
python videos_reduce.py mentalhealth_002.h5.repacked 6 8 /tmp/clickpy &
python videos_reduce.py mentalhealth_002.h5.repacked 7 8 /tmp/clickpy &
python videos_reduce.py mentalhealth_002.h5.repacked 8 8 /tmp/clickpy &
python proc_tmp.py /tmp/clickpy /tmp/clickpy-2.h5 &
