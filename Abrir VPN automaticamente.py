import pyautogui
import time

print("Posição para leitura...")
time.sleep(1)

x, y = pyautogui.position()
print("Current mouse coordinates:", x, y)

target_coords = (1123, 742)
# pyautogui.moveTo(target_coords, duration=0.2)
pyautogui.click(target_coords)
time.sleep(1)

target_coords_2 = (1034, 633)
# pyautogui.moveTo(target_coords_2, duration=0.2)
pyautogui.rightClick(target_coords_2) 
time.sleep(1)

target_coords_3 = (936, 385)
# pyautogui.moveTo(target_coords_2, duration=0.2)
pyautogui.click(target_coords_3)
time.sleep(1)