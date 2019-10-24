fbat=open("batout.txt","r")
fbowl=open("ballout.txt","r")
fall=open("ptop.txt","r")
fa=open("cluout.csv","w")
batc=fbat.readlines()
bowlc=fbowl.readlines()
fal=fall.readlines()
bocllis=[[],[],[],[],[],[],[],[],[],[]]
bacllis=[[],[],[],[],[],[],[],[],[],[]]
for ww in bowlc:
    w1=ww.split(",")
    j=int(w1[0])
    bocllis[j].append(w1[1])
for wb in batc:
    w2=wb.split(",")
    j=int(w2[0])
    bacllis[j].append(w2[1])
for iii in range(10):
    for jjj in range(10):
        zerp,onep,twop,thrp,foup,sixp,outp=0,0,0,0,0,0,0
        count=0
        for i in bacllis[iii]:
            for j in bocllis[jjj]:
                for k in fal:
                    kk=k.split(",")
                    if(kk[0]==i and kk[1]==j):
                        zerp+=float(kk[2])
                        onep+=float(kk[3])
                        twop+=float(kk[4])
                        thrp+=float(kk[5])
                        foup+=float(kk[6])
                        sixp+=float(kk[7])
                        outp+=float(kk[8])
                        count+=1
                        break
        try:
            ze=zerp/count
        except:
            ze=0
        try:
            on=onep/count
        except:
            on=0
        try:
            tw=twop/count
        except:
            tw=0
        try:
            th=thrp/count
        except:
            th=0
        try:
            fr=foup/count
        except:
            fr=0
        try:
            si=sixp/count
        except:
            si=0
        try:
            ou=outp/count
        except:
            ou=0
        fa.write(str(iii)+","+str(jjj)+","+str(ze)+","+str(on)+","+str(tw)+","+str(th)+","+str(fr)+","+str(si)+","+str(ou)+"\n")
        print(str(iii)+","+str(jjj)+","+str(ze)+","+str(on)+","+str(tw)+","+str(th)+","+str(fr)+","+str(si)+","+str(ou)+"\n")
