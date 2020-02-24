#!/usr/bin/env python3
# This is viewer for processlists collected by pymysqlphs utility
# License GNU Public License GPL-2.0 http://opensource.org/licenses/gpl-2.0
# Created by Eugene K., 2019
import pymysql
import datetime
import os
import time
import hashlib
import curses
import re
from settings import *

def readfilelist():
    files = []
    for r, d, f in os.walk(snapdirectory):
       for file in f:
           if re.match(filemask, file):
               files.append(os.path.join(r, file))
    files.sort()
    return(files)

def getfilelines(filename):
    f=open(filename,"r")
    lines=[]
    for i in f:
        lines.append(i)
    f.close()
    return lines

def display(stdscr):
    stdscr.clear()
    files=readfilelist()
    listposition=len(files)-1
    maxlistposition=len(files)-1
    skiplines=0
    while True:
        try:
            curses.noecho()
            curses.cbreak()
            stdscr.keypad(True)
            lines=getfilelines(files[listposition])
            stdscr.addstr(0, 0, "FILE: %s" % files[listposition], curses.A_REVERSE)
            posv=1
            try:
                ln=0 #variable for current line number
                for l in lines:
                    if ln>=skiplines:
                        stdscr.addstr(posv, 0, "%s"%l, curses.A_NORMAL)
                        posv=posv+1
                        ln=ln+1
                    else:
                        ln=ln+1
                stdscr.refresh()
            except: 
                #in most cases we do have this exception - we pring text outside the screen
                pass
            keypressed = stdscr.getch()
            if keypressed == 27: #ESC key pressed
                             exit()
            if keypressed == 113: #key 'q' pressed
                             exit()
            elif keypressed == curses.KEY_RIGHT:
                             listposition=listposition+1
                             stdscr.clear()
                             skiplines=0
                             if (listposition>=maxlistposition):
                                 listposition=maxlistposition
            elif keypressed == curses.KEY_LEFT:
       	       	       	     listposition=listposition-1
                             stdscr.clear()
                             skiplines=0
       	       	       	     if	(listposition<=0):
       	       	       	       	 listposition=0
            elif keypressed == curses.KEY_UP:
                             skiplines=skiplines-1
                             if skiplines<=0:
                                 skiplines
            elif keypressed == curses.KEY_DOWN:
                             skiplines=skiplines+1
                             if (skiplines>=(len(lines)-1)):
                                 skiplines=len(lines)-1
            else:
                time.sleep(0.1)
        except Exception as e:
            stdscr.addstr(0,0,"%s" % format(e))
            stdscr.refresh()
#           curses.nocbreak()
#	    stdscr.keypad(False)
#	    curses.echo()
#	    curses.endwin()
            return

curses.wrapper(display)
