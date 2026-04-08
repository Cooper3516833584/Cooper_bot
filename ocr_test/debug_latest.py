import sys
sys.path.insert(0, '.')
import cv2
from blackboard_ocr import (build_ocr, get_board, enhance_variants,
    run_ocr_on_image, group_items_to_lines, extract_assignments_from_lines,
    merge_variant_assignments)

engine = build_ocr()

for img_path in ['ocr_test/95F8093BC0C15E05FED8620B4061E63E.jpg', 'ocr_test/D3A4234773454FC8110CB6EE2BFFB5FD.jpg']:
    print(f"\n======== {img_path} ========")
    img = cv2.imread(img_path)
    board = get_board(img)
    variant_results = []
    
    for vname, vimg in enhance_variants(board).items():
        items = run_ocr_on_image(engine, vimg)
        lines = group_items_to_lines(items)
        asn = extract_assignments_from_lines(lines)
        variant_results.append({'variant': vname, 'assignments': asn})
        print(f'\n[{vname}]')
        for a in asn:
            print(f'  {a}')

    print('\nMERGED:')
    merged = merge_variant_assignments(variant_results)
    for a in merged:
        print(f'  {a}')
