"""debug_9jpg.py - 调试 9.jpg 的 1.6.11 幻觉问题"""
import sys
sys.path.insert(0, '.')
import cv2
from blackboard_ocr import (build_ocr, get_board, enhance_variants,
    run_ocr_on_image, group_items_to_lines, extract_assignments_from_lines,
    merge_variant_assignments, is_green_blackboard)

engine = build_ocr()
img = cv2.imread('ocr_test/9.jpg')
print(f'9.jpg shape={img.shape}')
board = get_board(img)
variant_results = []
for vname, vimg in enhance_variants(board).items():
    items = run_ocr_on_image(engine, vimg)
    lines = group_items_to_lines(items)
    asn = extract_assignments_from_lines(lines)
    variant_results.append({'variant': vname, 'assignments': asn})
    print(f'[{vname}]')
    for a in asn:
        print(f'  {a}')

print('\nMERGED:')
merged = merge_variant_assignments(variant_results)
for a in merged:
    print(f'  {a}')
