#!/usr/bin/env python3
"""Create or update the conversion performance comment on a Gitea pull request."""
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

MARKER = '<!-- pandata-performance -->'


def body(summary, base, head, run_url):
    path = Path(summary)
    if path.is_file():
        text = path.read_text()
    else:
        text = f'{MARKER}\n### Conversion performance\n\nComparison failed; no measurements were produced.\n'
    return f'{text}\nBase `{base[:10]}` · Head `{head[:10]}` · [Run logs]({run_url})\n'


def find_comment(comments):
    return next((c['id'] for c in comments if MARKER in (c.get('body') or '')), None)


def request(method, url, token, payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={
        'Authorization': f'token {token}',
        'Accept': 'application/json',
        'Content-Type': 'application/json',
    })
    with urllib.request.urlopen(req, timeout=30) as response:
        return json.loads(response.read() or 'null')


def main():
    env = os.environ
    api = f"{env['API_URL'].rstrip('/')}/repos/{env['REPO']}"
    token = env['GITEA_TOKEN']
    text = body(env['SUMMARY'], env['BASE'], env['HEAD'], env['RUN_URL'])
    try:
        # Gitea returns every issue comment in one unpaginated response.
        existing = find_comment(request('GET', f"{api}/issues/{env['PR']}/comments", token))
        if existing is None:
            request('POST', f"{api}/issues/{env['PR']}/comments", token, {'body': text})
        else:
            request('PATCH', f'{api}/issues/comments/{existing}', token, {'body': text})
    except (urllib.error.URLError, OSError, ValueError) as error:
        # Reporting is best effort; never fail the job over a comment.
        print(f'::warning::Could not post performance comment: {error}', file=sys.stderr)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
