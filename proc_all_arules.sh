cd /src/clickpy/prep_sequences
python /src/clickpy/prep_sequences/prep_sequences.py /data/clickstream/h5/001_aboriginaled.h5 /data/arules/001_aboriginaled --tmpdir /bigdisk/shared/tmp/cl --procs 8 --force --cutoff 1364947200
python /src/clickpy/prep_sequences/prep_sequences.py /data/clickstream/h5/intropsych_001.h5 /data/arules/intropsych_001 --tmpdir /bigdisk/shared/tmp/cl --procs 8 --force --cutoff 1373587200
python /src/clickpy/prep_sequences/prep_sequences.py /data/clickstream/h5/introstats_001.h5 /data/arules/introstats_001 --tmpdir /bigdisk/shared/tmp/cl --procs 8 --force --cutoff 1370563200
python /src/clickpy/prep_sequences/prep_sequences.py /data/clickstream/h5/mentalhealth_001.h5 /data/arules/mentalhealth_001 --tmpdir /bigdisk/shared/tmp/cl --procs 8 --force --cutoff 1363824000
python /src/clickpy/prep_sequences/prep_sequences.py /data/clickstream/h5/mentalhealth_002.h5 /data/arules/mentalhealth_002 --tmpdir /bigdisk/shared/tmp/cl --procs 8 --force --cutoff 1376524800
python /src/clickpy/prep_sequences/prep_sequences.py /data/clickstream/h5/programming1_002.h5 /data/arules/programming1_002 --tmpdir /bigdisk/shared/tmp/cl --procs 8 --force --cutoff 1377475200
