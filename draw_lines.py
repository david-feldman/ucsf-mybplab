from PIL import Image, ImageDraw
import os
imgs = os.listdir('body_map_images')
imgs = [i for i in imgs if 'annotated' not in i]
for img in imgs:
    im = Image.open('./body_map_images/'+img)
    draw = ImageDraw.Draw(im)
    print(img, im.size)
    draw.line((0,im.size[1]/6.5,im.size[0],im.size[1]/6.5), fill='red', width=5)
    draw.line((0,im.size[1]/2.4,im.size[0],im.size[1]/2.4), fill='red', width=5)
    im.save('./body_map_images/'+img[0:-4] + '_annotated.png')

#draw.line((0, 0) + im.size, fill=128)
#draw.line((0, im.size[1], im.size[0], 0), fill=128)
#del draw

