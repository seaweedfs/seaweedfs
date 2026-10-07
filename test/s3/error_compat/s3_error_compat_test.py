#!/usr/bin/env python3
"""Verify S3/IAM error responses match AWS for common client mistakes.

Needs only botocore (pip install botocore). Sends raw SigV4 requests so the exact
status and response body are visible, compares each with what AWS returns, then
cleans up everything it created (one bucket, one IAM user).

Usage:
  python3 s3_error_compat_test.py --endpoint http://127.0.0.1:8333 \
      --access-key <admin key> --secret-key <admin secret>

The key needs Admin rights, and IAM writes must be enabled (-s3.iam.readOnly=false).
"""
import argparse, atexit, base64, datetime, hashlib, http.client, re, sys, time, urllib.parse, uuid

from botocore.auth import S3SigV4Auth, SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

ap = argparse.ArgumentParser()
ap.add_argument('--endpoint', default='http://127.0.0.1:8333')
ap.add_argument('--access-key', required=True)
ap.add_argument('--secret-key', required=True)
ap.add_argument('--region', default='us-east-1')
args = ap.parse_args()

EP = args.endpoint.rstrip('/')
U = urllib.parse.urlsplit(EP)
ADMIN = Credentials(args.access_key, args.secret_key)
BAD_KEY = Credentials('AKIAREPRONOSUCHKEY00', 'x' * 40)
BAD_SECRET = Credentials(args.access_key, 'y' * 40)
B = 'errorcompat-' + uuid.uuid4().hex[:8]
S3DOC = 'https://docs.aws.amazon.com/AmazonS3/latest/API/'
IAMDOC = 'https://docs.aws.amazon.com/IAM/latest/APIReference/'
rows = []


def send(method, path, body=b'', headers=None, creds=ADMIN, service='s3', mutate=None, sign=True,
         keep_sha256_header=False):
    headers = dict(headers or {})
    url = EP + path
    if sign:
        req = AWSRequest(method=method, url=url, data=body, headers=headers)
        if service == 's3':
            if 'X-Amz-Content-SHA256' not in req.headers:
                req.headers['X-Amz-Content-SHA256'] = hashlib.sha256(body).hexdigest()
            # S3SigV4Auth always recomputes X-Amz-Content-SHA256 from the body; the
            # generic SigV4Auth signs a caller-supplied value instead, which is how
            # a mismatched declared hash can be produced on the wire.
            signer = SigV4Auth if keep_sha256_header else S3SigV4Auth
            signer(creds, 's3', args.region).add_auth(req)
        else:
            SigV4Auth(creds, service, args.region).add_auth(req)
        headers = dict(req.headers.items())
    if mutate:
        headers = mutate(headers)
    conn = http.client.HTTPConnection(U.hostname, U.port, timeout=30)
    conn.request(method, path, body=body, headers=headers)
    r = conn.getresponse()
    data = r.read().decode('utf-8', 'replace')
    conn.close()
    return r.status, data


def shape(data):
    """Root element and <Code> of an XML error body."""
    body = re.sub(r'<\?xml[^>]*\?>', '', data).strip()
    root = re.match(r'<(\w+)', body)
    code = re.search(r'<Code>([^<]*)</Code>', data)
    return (root.group(1) if root else ('-' if not body else 'non-xml')), (code.group(1) if code else '')


def iam(params, creds=ADMIN):
    p = dict(params, Version='2010-05-08')
    return dict(method='POST', path='/', body=urllib.parse.urlencode(p).encode(), creds=creds, service='iam',
                headers={'Content-Type': 'application/x-www-form-urlencoded; charset=utf-8'})


def case(group, name, expect, ref, req, check=None):
    """expect = (status, root element or None, code or None); check() adds a side-effect note."""
    st, data = send(**req)
    root, code = shape(data)
    note = check() if check else ''
    ok = st == expect[0] and expect[1] in (None, root) and expect[2] in (None, code) and not note
    rows.append((group, name, '%s %s %s' % (st, root, code), '%s %s %s' % (expect[0], expect[1] or '', expect[2] or ''), note, ok, ref))


def exists(key):
    return lambda: 'object %r now exists' % key if send('HEAD', '/%s/%s' % (B, key))[0] == 200 else ''


def S3(method, path, **kw):
    return dict(method=method, path=path, **kw)


# ---------------- cleanup (atexit so failed runs still clean up) ----------------
user = USER = upload_id = None


def cleanup():
    try:
        if user and USER:
            send(**iam({'Action': 'DeleteAccessKey', 'UserName': user, 'AccessKeyId': USER.access_key}))
        if user:
            send(**iam({'Action': 'DeleteUser', 'UserName': user}))
        if upload_id:
            send('DELETE', '/%s/mp?uploadId=%s' % (B, upload_id))
        st, d = send('GET', '/%s?list-type=2' % B)
        for k in re.findall(r'<Key>([^<]+)</Key>', d):
            send('DELETE', '/%s/%s' % (B, urllib.parse.quote(k)))
        send('DELETE', '/' + B)
    except Exception as e:
        print('cleanup failed: %s' % e)


atexit.register(cleanup)

# ---------------- setup ----------------
st, d = send('PUT', '/' + B)
if st != 200:
    sys.exit('cannot create bucket %s: %s %s' % (B, st, d[:200]))
send('PUT', '/%s/obj' % B, body=b'hello')
st, d = send('POST', '/%s/mp?uploads' % B)
upload_id = re.search(r'<UploadId>([^<]+)', d).group(1)
user = 'errorcompat-' + uuid.uuid4().hex[:6]
send(**iam({'Action': 'CreateUser', 'UserName': user}))
st, d = send(**iam({'Action': 'CreateAccessKey', 'UserName': user}))
USER = Credentials(re.search(r'<AccessKeyId>([^<]+)', d).group(1), re.search(r'<SecretAccessKey>([^<]+)', d).group(1))
for _ in range(15):  # wait until the new key is accepted
    if 'InvalidAccessKeyId' not in send('GET', '/', creds=USER)[1]:
        break
    time.sleep(1)

# ---------------- 1. requests that run a different operation ----------------
for sub in ('logging', 'notification', 'accelerate', 'website', 'replication',
            'analytics&id=a', 'inventory&id=a', 'metrics&id=a', 'intelligent-tiering&id=a'):
    case(1, 'PUT ?%s' % sub, (501, 'Error', 'NotImplemented'), S3DOC + 'API_Error.html#:~:text=Code%3A%20NotImplemented',
         S3('PUT', '/%s?%s' % (B, sub), body=b'<X/>'))
case(1, 'DELETE ?logging', (501, 'Error', 'NotImplemented'), S3DOC + 'API_Error.html#:~:text=Code%3A%20NotImplemented',
     S3('DELETE', '/%s?logging' % B),
     lambda: '' if send('HEAD', '/' + B)[0] == 200 else 'BUCKET DELETED')
case(1, 'CopyObject, x-amz-copy-source without "/"', (400, 'Error', 'InvalidArgument'),
     S3DOC + 'API_CopyObject.html#AmazonS3-CopyObject-request-header-CopySource',
     S3('PUT', '/%s/copy-dst' % B, headers={'x-amz-copy-source': 'nobucketonly'}), exists('copy-dst'))
case(1, 'UploadPart partNumber=abc', (400, 'Error', 'InvalidArgument'),
     S3DOC + 'API_UploadPart.html#AmazonS3-UploadPart-request-uri-querystring-PartNumber',
     S3('PUT', '/%s/mp?partNumber=abc&uploadId=%s' % (B, upload_id), body=b'part-body'), exists('mp'))
case(1, 'PutObject, x-amz-content-sha256 != body', (400, 'Error', 'XAmzContentSHA256Mismatch'),
     'undocumented code; ' + S3DOC + 'API_UploadPart.html#API_UploadPart_RequestSyntax',
     S3('PUT', '/%s/sha-mismatch' % B, body=b'abc',
        headers={'X-Amz-Content-SHA256': hashlib.sha256(b'zzz').hexdigest()}, keep_sha256_header=True),
     exists('sha-mismatch'))
case(1, 'PutObject, x-amz-content-sha256 not hex', (400, 'Error', None), 'undocumented',
     S3('PUT', '/%s/sha-nothex' % B, body=b'abc',
        headers={'X-Amz-Content-SHA256': 'nothex'}, keep_sha256_header=True), exists('sha-nothex'))

# ---------------- 2. IAM failures in the S3 <Error> envelope ----------------
case(2, 'IAM ListUsers, unknown access key', (403, 'ErrorResponse', 'InvalidClientTokenId'),
     'checked against iam.amazonaws.com (docs list UnrecognizedClientException); ' + IAMDOC + 'CommonErrors.html#CommonErrors-UnrecognizedClientException', iam({'Action': 'ListUsers'}, creds=BAD_KEY))
case(2, 'IAM ListUsers, wrong secret', (403, 'ErrorResponse', None), 'undocumented for IAM',
     iam({'Action': 'ListUsers'}, creds=BAD_SECRET))
case(2, 'IAM ListUsers, non-admin user', (403, 'ErrorResponse', 'AccessDenied'),
     IAMDOC + 'CommonErrors.html#CommonErrors-AccessDeniedException', iam({'Action': 'ListUsers'}, creds=USER))
case(2, 'IAM CreateUser, non-admin user', (403, 'ErrorResponse', 'AccessDenied'),
     IAMDOC + 'CommonErrors.html#CommonErrors-AccessDeniedException', iam({'Action': 'CreateUser', 'UserName': user + 'x'}, creds=USER))

# ---------------- 3. wrong status or code ----------------
tags = [('Action', 'TagUser'), ('UserName', user)] + [('Tags.member.%d.%s' % (i, k), 'k%d' % i if k == 'Key' else 'v')
                                                       for i in range(1, 52) for k in ('Key', 'Value')]
case(3, 'IAM TagUser with 51 tags', (409, 'ErrorResponse', 'LimitExceeded'), IAMDOC + 'API_TagUser.html#API_TagUser_Errors',
     iam(dict(tags)))
case(3, 'IAM unknown Action', (404, 'ErrorResponse', 'InvalidAction'), 'checked against iam.amazonaws.com',
     iam({'Action': 'NoSuchActionZZ'}))


def no_date(h):
    return {k: v for k, v in h.items() if k.lower() != 'x-amz-date'}


def drop(field):
    return lambda h: dict(h, Authorization=re.sub(r',? *%s=[^,]*' % field, '', h['Authorization']))


case(3, 'S3 request without x-amz-date', (403, 'Error', 'AccessDenied'), 'checked against s3.amazonaws.com; ' + S3DOC + 'API_Error.html#:~:text=Code%3A%20AccessDenied',
     S3('GET', '/' + B, mutate=no_date))
case(3, 'Authorization without Signature=', (400, 'Error', 'AuthorizationHeaderMalformed'),
     'checked against s3.amazonaws.com; ' + S3DOC + 'API_Error.html#:~:text=Code%3A%20AuthorizationHeaderMalformed', S3('GET', '/' + B, mutate=drop('Signature')))
case(3, 'Authorization without Credential=', (400, 'Error', 'InvalidArgument'),
     'checked against s3.amazonaws.com ("Unsupported Authorization Type")', S3('GET', '/' + B, mutate=drop('Credential')))
for m, exp, chk in (('GET', (400, 'Error', 'InvalidArgument'), 'checked against s3.amazonaws.com ("Invalid version id specified"); '),
                    ('HEAD', (400, None, None), 'checked against s3.amazonaws.com; '),
                    ('DELETE', (400, None, None), 'unverified (needs write access); ')):
    case(3, '%s ?versionId=bogus, unversioned bucket' % m, exp,
         chk + S3DOC + 'API_GetObject.html#AmazonS3-GetObject-request-uri-querystring-VersionId',
         S3(m, '/%s/obj?versionId=bogus' % B))
for pn in ('0', '10001'):
    case(3, 'UploadPart partNumber=%s' % pn, (400, 'Error', 'InvalidArgument'),
         S3DOC + 'API_UploadPart.html#AmazonS3-UploadPart-request-uri-querystring-PartNumber',
         S3('PUT', '/%s/mp?partNumber=%s&uploadId=%s' % (B, pn, upload_id), body=b'x'))
case(3, 'DeleteObjects with 1001 keys', (400, 'Error', 'MalformedXML'), S3DOC + 'API_DeleteObjects.html#API_DeleteObjects_RequestBody',
     S3('POST', '/%s?delete' % B, body=('<Delete>' + ''.join('<Object><Key>k%d</Key></Object>' % i for i in range(1001)) + '</Delete>').encode()))
# last: this one deletes the user when it should be refused
case(3, 'IAM DeleteUser while it has an access key', (409, 'ErrorResponse', 'DeleteConflict'),
     IAMDOC + 'API_DeleteUser.html#API_DeleteUser_Errors', iam({'Action': 'DeleteUser', 'UserName': user}))

# ---------------- report ----------------
w = max(len(r[1]) for r in rows)
print('%-4s %-*s  %-46s %-40s %s' % ('', w, 'case', 'got (status root code)', 'expected (AWS)', 'side effect'))
for g, name, got, exp, note, ok, ref in rows:
    print('%-4s %-*s  %-46s %-40s %s' % ('ok' if ok else 'DIFF', w, name, got, exp, note))
print('\nreferences:')
for g, name, got, exp, note, ok, ref in rows:
    print('  %-*s  %s' % (w, name, ref))
diff = sum(not r[5] for r in rows)
print('\n%d cases, %d differ from AWS' % (len(rows), diff))
sys.exit(1 if diff else 0)
