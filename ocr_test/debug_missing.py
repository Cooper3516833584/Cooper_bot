"""debug_missing.py - 快速调试漏识别和页码错误的图片"""
import sys
sys.path.insert(0, '.')
import cv2
from blackboard_ocr import (build_ocr, get_board, enhance_variants,
    run_ocr_on_image, group_items_to_lines, extract_assignments_from_lines,
    is_green_blackboard, merge_variant_assignments, recognize_homework_from_path)

engine = build_ocr()

# 1. 检查完全漏识别的图
for img_name in ['18.jpg', '27.jpg']:
    img = cv2.imread(f'ocr_test/{img_name}')
    print(f'\n=== {img_name}: shape={img.shape}, is_board={is_green_blackboard(img)}')
    board = get_board(img)
    print(f'    board shape={board.shape}')
    for vname, vimg in list(enhance_variants(board).items()):
        items = run_ocr_on_image(engine, vimg)
        lines = group_items_to_lines(items)
        asn = extract_assignments_from_lines(lines)
        print(f'  [{vname}] items={len(items)} assignments={asn}')
        for ln in lines[:4]:
            print(f'    line score={ln["score"]:.2f} | {ln["text"]}')

# 2. 检查 P206/P208 错误的图
print('\n\n=== 13.jpg page investigation ===')
img = cv2.imread('ocr_test/13.jpg')
board = get_board(img)
variant_results = []
for vname, vimg in enhance_variants(board).items():
    items = run_ocr_on_image(engine, vimg)
    lines = group_items_to_lines(items)
    asn = extract_assignments_from_lines(lines)
    variant_results.append({'variant': vname, 'assignments': asn})
    print(f'\n[{vname}]')
    for ln in lines:
        print(f'  line: {ln["score"]:.2f} | {ln["text"]}')
    print(f'  assignments: {asn}')

merged = merge_variant_assignments(variant_results)
print('\nMERGED:', merged)
