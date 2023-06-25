import pyautogui
import time

def hack_time_doctor(): 
    while(True):
        pyautogui.typewrite("Hello geeks !")
        time.sleep(5)
        pyautogui.moveTo(1000, 1000, duration = 1)
        time.sleep(5)

if __name__ == "__main__":
    hack_time_doctor()