import sys
import ntpath

splitLen = 45000         # 45000 lines per file
outputBase = 'train_output' # output.1.txt, output.2.txt, etc.
print(ntpath.exists('E:/Kaggle/Display Advertising Challenge/train/'))



input = open('E:/Kaggle/Display Advertising Challenge/train/train.csv', 'r')

count = 0
at = 0
dest = None
for line in input:
    if count % splitLen == 0:
        if dest: dest.close()
        dest = open('E:/Kaggle/Display Advertising Challenge/train/'+outputBase + str(at) + '.txt', 'w')
        at += 1
    dest.write(line)
    count += 1