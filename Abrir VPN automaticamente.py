import pyautogui as pg
import time

arrow_image = "E:/GitHub/FernandoSilva33-1/arrow.png"
arrow_location = pg.locateOnScreen(arrow_image)

# If the arrow is found, click on it
if arrow_location is not None:
  pg.rightClick(arrow_location.left + 20, arrow_location.top + 20, duration=2)
else:
    print("Could not find the 'Show hidden items' arrow.")