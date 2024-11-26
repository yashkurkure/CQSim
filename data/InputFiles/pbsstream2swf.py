import pandas as pd

swf_columns = [
    'id',             #1
    'submit',         #2
    'wait',           #3
    'run',            #4
    'used_proc',      #5
    'used_ave_cpu',   #6
    'used_mem',       #7
    'req_proc',       #8
    'req_time',       #9
    'req_mem',        #10 
    'status',         #11
    'user_id',        #12
    'group_id',       #13
    'num_exe',        #14
    'num_queue',      #15
    'num_part',       #16
    'num_pre',        #17
    'think_time',     #18
]

df = pd.read_csv("pbsstream.csv", names=swf_columns)
df['id'] += 1
df.to_csv('pbsstream_adj_id.csv', sep=',', index=False, header=False)  

df = df.sort_values(by='id')

# CQSim hates job id 0 :(

with open('pbsstream.swf', 'w') as f:
    f.write('; MaxNodes: 75\n')
    f.write('; MaxProcs: 75\n')
    for index, row in df.iterrows():
        line = ' '.join(str(val) for val in row.values)
        f.write(f'{line}\n')