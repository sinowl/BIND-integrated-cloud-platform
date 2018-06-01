__author__ = 'yoonsu'

from src.Abstract import *

exe=Abstract()

#input1_value = {'work': 'twitter', 'term': 'restaurant'}
#output1_value = {'work': 'mapreduce', 'map': 'map.py', 'reduce': 'reduce.py', 'source': 'input1'}
#output2_value = {'work': 'script', 'file': 'display_result.py', 'source':'output1'}

#input2_value = {'work': 'twitter', 'term': 'cafe'}
#output3_value = {'work': 'script', 'file': 'show_map.py'}

#exe.pyquery(input1=input1_value, output1=output1_value, output2=output2_value)
#exe.pyquery(input2=input2_value) #, output1=output1_value, output3=output3_value)

#exe.pyquery2('input2').work('crawling').html('http://www.naver.com').getTag('div')
									# getTerm('web')
					# htmlall('http://www.naver.com')
"""
exe.pyquery2('input1').work('twitter').term('restaurant').time(60).splitNum(2)#.getnodeContents()
exe.pyquery2('output1').source('input1').work('mapreduce').map( 'map.py' ).reduce( 'reduce.py' )#.getnodeContents()
exe.pyquery2('output2').source('output1').work('script').file( 'display_result.py' )#.getnodeContents()
"""

exe.pyquery2('outputML').source('inputML').work('ML').file('conv_net_sentence.py').reffile('conv_net_classes.py').cmd('THEANO_FLAGS=mode=FAST_RUN,device=cpu,floatX=float32 python conv_net_sentence.py -static -word2vec').parallelNum(4)


#exe.showNode()
#exe.displaytopology()
#exe.start()
#exe.displaytopology()
exe.run()

"""
d1 = pyquery([  './input1' : {  'work':'twitter'    },
                             '/output1' : {  'work':'mapreduce', 'map':'map.py', 'reduce':'reduce.py' },
                                          '/output2' : {  'work':'script', 'file':'display_result.py' },
            ])
"""
