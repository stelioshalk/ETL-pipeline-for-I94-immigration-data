import os
#run once to generate the csv files from text files : i94cntyl  i94addrl i94prtl.
#process i94cntyl.txt
i94cntylFile = open('txtfiles/i94cntyl.txt', 'r')
i94cntylFilecsv = open('i94cntyl.csv', 'w')
try:
    while True:
        line = i94cntylFile.readline() 
        if not line:
            break
        list=line.split("=")
        newline=list[0].strip()+","+list[1].replace("'","").replace(","," ")
        i94cntylFilecsv.write(newline)
except IndexError:
    print("empty line->"+line)
i94cntylFilecsv.close()

#process i94addrl.txt
i94addrlFile = open('txtfiles/i94addrl.txt', 'r')
i94addrlFilecsv = open('i94addrl.csv', 'w')
try:
    while True:
        line = i94addrlFile.readline()
        if not line:
            break
        list=line.split("=")
        newline=list[0].strip().replace("'","")+","+list[1].replace("'","").replace(","," ")
        i94addrlFilecsv.write(newline)
except IndexError:
    print("empty line->"+line)
i94addrlFilecsv.close()

#process i94prtl.txt
i94prtlFile = open('txtfiles/i94prtl.txt', 'r')
i94prtllFilecsv = open('i94prtl.csv', 'w')
try:
    while True:
        line = i94prtlFile.readline()
        if not line:
            break
        list=line.split("=")       
        arport_code=list[0].strip().replace("'","")        
        secondpart=list[1].split(",")        
        city=secondpart[0].strip().replace("'","")
        if len(secondpart)==2:
            state_code=secondpart[1].replace("'","").replace(" ","")
        else:
            state_code=""
        newline=arport_code+","+city+","+state_code#+"\n"
        i94prtllFilecsv.write(newline)
except IndexError:
    print("empty line->"+line)
i94prtllFilecsv.close()