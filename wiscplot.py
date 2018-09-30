# import the library
from zplot import *
import sys

def draw(filename1,filename2,filename3,filename4,outputname,yfd,yr,ya,yt):
             # drawing surface
             ctype = 'pdf'
             c = canvas(ctype, title=outputname, dimensions=['3.3in', '2.4in'])

             # load data
             t1 = table(file=filename1)
             t2 = table(file=filename2)
             t3 = table(file=filename3)
             t4 = table(file=filename4)
             # drawable region
             d = drawable(canvas=c, xrange=[0.5,6.5], yrange=yr,
                          coord=['0.5in','0.4in'], dimensions=['2.6in','1.7in'])

             # xlabel for value size
             xlabels = [['64B', 1], ['256B', 2], ['1KB', 3], ['4KB', 4],['16KB', 5],
                          ['60KB', 6]]

             # axes
             axis(drawable=d, xtitle='Key: 16B, Value: 64B to 60KB',
                  ytitle=yt, xmanual=xlabels, xlabelfontsize=8, xtitlesize=8,
                  ylabelfontsize=8, ytitlesize=8, yauto=ya)

             # plot and legent
             p = plotter()
             L = legend()
             p.points(drawable=d, table=t1, xfield='x', size=2.5, yfield=yfd, style='circle',
                          legend=L, legendtext='WiscKey')
             p.line(drawable=d,linedash=[2,2], table=t1, xfield='x', yfield=yfd)

             p.points(drawable=d, table=t4, xfield='x', size=2.5, yfield=yfd, style='utriangle',
                          legend=L, legendtext='WiscKey-Hybrid')
             p.line(drawable=d, table=t4, xfield='x', yfield=yfd)

             p.points(drawable=d, table=t2, xfield='x', size=2.5, yfield=yfd, style='xline',
                          legend=L, legendtext='WiscKey-ValueThreshold')
             p.line(drawable=d,linedash=[4,1], table=t2, xfield='x', yfield=yfd)

             p.points(drawable=d, table=t3, xfield='x', size=2.5, yfield=yfd, style='diamond',
                          legend=L, legendtext='WiscKey-LevelDBSet')
             p.line(drawable=d,linedash=[3,3], table=t3, xfield='x', yfield=yfd)

             L.draw(canvas=c, width=8.0, height=8.0, skipnext=2,skipspace=80.0,coord=[d.left()+10, d.top()+10], fontsize=8)

             # final output
             c.render()

draw('load1.data','load2.data','load3.data','load4.data','wisckeyseqlload','yseql',[0,250],['','',25],'Throughput (MB/s)')
draw('load1.data','load2.data','load3.data','load4.data','wisckeyranload', 'yranl', [0,250], ['','',25],'Throughput (MB/s)')
draw('load1.data','load2.data','load3.data','load4.data','wisckeyspace', 'size', [1000,3000], ['','',250],'DataBase Size (MB)')
draw('load1.data','load2.data','load3.data','load4.data','wisckeyrangeqforseq', 'sseqq', [0,300], ['','',30],'Throughput (MB/s)')
draw('load1.data','load2.data','load3.data','load4.data','wisckeyrangeqforran', 'rseqq', [0,300], ['','',30],'Throughput (MB/s)')
draw('load1.data','load2.data','load3.data','load4.data','wisckeyranlookup', 'rranq', [0,200], ['','',20],'Throughput (MB/s)')
