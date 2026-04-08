import sys
sys.path.insert(0, '.')
import json
import traceback
from blackboard_ocr import build_ocr, recognize_homework_from_path

engine = build_ocr()
images = [
    'ocr_test/95F8093BC0C15E05FED8620B4061E63E.jpg',
    'ocr_test/D3A4234773454FC8110CB6EE2BFFB5FD.jpg'
]

with open('ocr_test/new_imgs_out.txt', 'w', encoding='utf-8') as f:
    for img_path in images:
        f.write(f"\n--- Processing {img_path} ---\n")
        try:
            result = recognize_homework_from_path(img_path, engine=engine)
            f.write(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
        except Exception as e:
            f.write(f"Exception: {e}\n")
            f.write(traceback.format_exc() + '\n')
