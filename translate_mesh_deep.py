"""
Translate MeSH terms using deep-translator (more stable, less rate-limit).
Translates both `term_en` and `definition` to Vietnamese.

Usage:
  python translate_mesh_deep.py --input mesh_terms.json --output mesh_terms_vi.jsonl
"""

import argparse
import json
import os
import time
import sys
from typing import Optional

try:
    from deep_translator import GoogleTranslator
except Exception as e:
    print(f"deep-translator not found: {e}")
    print("Install: pip install deep-translator")
    sys.exit(1)


def load_cache(path):
    if os.path.exists(path):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_cache(cache, path):
    try:
        tmp = path + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except Exception as e:
        print(f'Warning: Failed to save cache: {e}')


def translate_text(text: str, cache: dict, retries=5, base_delay=1.0) -> Optional[str]:
    """Translate text with retry logic and exponential backoff."""
    if not text or not text.strip():
        return None
    
    if text in cache:
        return cache[text]

    translator = GoogleTranslator(source='en', target='vi')
    
    for attempt in range(retries):
        try:
            translated = translator.translate(text)
            if translated:
                cache[text] = translated
                return translated
        except Exception as e:
            if attempt < retries - 1:
                wait = base_delay * (2 ** attempt)  # exponential backoff
                print(f'Retry {attempt + 1}/{retries} in {wait}s: {str(e)[:40]}...')
                time.sleep(wait)
            else:
                print(f'Failed after {retries} retries: "{text[:40]}"')
                return None
    
    return None


def process(input_path, output_path, cache_path, delay=0.5, resume=True):
    """Process input JSON and produce translated JSONL."""
    # load cache
    cache = load_cache(cache_path)
    print(f'Loaded {len(cache)} cache entries from {cache_path}')
    print(f'Input: {input_path}')
    print(f'Output: {output_path}')

    # Prepare output file in append mode
    out_mode = 'a' if resume else 'w'
    written = 0
    existing_ids = set()
    
    if resume and os.path.exists(output_path):
        try:
            with open(output_path, 'r', encoding='utf-8') as of:
                for line in of:
                    try:
                        obj = json.loads(line)
                        if 'mesh_id' in obj:
                            existing_ids.add(obj['mesh_id'])
                    except Exception:
                        continue
            print(f'Resume mode: skipping {len(existing_ids)} existing entries')
        except Exception:
            pass

    # Stream-read the JSON array
    with open(input_path, 'r', encoding='utf-8') as f_in, open(output_path, out_mode, encoding='utf-8') as f_out:
        buffer = ''
        in_array = False
        brace_count = 0
        current = ''
        total_read = 0
        
        while True:
            chunk = f_in.read(65536)
            if not chunk:
                break
            
            for ch in chunk:
                if not in_array:
                    if ch == '[':
                        in_array = True
                    continue
                
                # accumulate object
                if ch == '{':
                    brace_count += 1
                if brace_count > 0:
                    current += ch
                if ch == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        # complete object
                        try:
                            obj = json.loads(current)
                        except Exception as e:
                            print(f'JSON parse error, skipping')
                            current = ''
                            continue
                        current = ''
                        total_read += 1

                        mesh_id = obj.get('mesh_id')
                        if resume and mesh_id and mesh_id in existing_ids:
                            # skip
                            continue

                        # Translate term_en
                        term_en = obj.get('term_en')
                        if term_en:
                            term_vi = translate_text(term_en, cache, retries=5)
                            if term_vi:
                                obj['term_vi'] = term_vi
                        
                        # Sleep
                        time.sleep(delay)

                        # Translate definition
                        definition = obj.get('definition')
                        if definition:
                            definition_vi = translate_text(definition, cache, retries=5)
                            if definition_vi:
                                obj['definition_vi'] = definition_vi
                        
                        # Sleep
                        time.sleep(delay)

                        # write as JSON line
                        f_out.write(json.dumps(obj, ensure_ascii=False) + '\n')
                        written += 1

                        # persist cache periodically
                        if written % 50 == 0:
                            save_cache(cache, cache_path)
                            f_out.flush()
                            print(f'Progress: {written} written (read {total_read} total)')

    # final save
    save_cache(cache, cache_path)
    print(f'\n=== Done ===')
    print(f'Total written: {written}')
    print(f'Total read: {total_read}')
    print(f'Output: {output_path}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Translate MeSH terms to Vietnamese using deep-translator')
    parser.add_argument('--input', '-i', default='mesh_terms.json', help='Input JSON array file')
    parser.add_argument('--output', '-o', default='mesh_terms_vi.jsonl', help='Output JSONL file')
    parser.add_argument('--cache', '-c', default='mesh_translate_cache.json', help='Cache file path')
    parser.add_argument('--delay', type=float, default=0.5, help='Delay (seconds) between API calls')
    parser.add_argument('--no-resume', dest='resume', action='store_false', help='Do not resume, start fresh')
    args = parser.parse_args()

    process(args.input, args.output, args.cache, delay=args.delay, resume=args.resume)
