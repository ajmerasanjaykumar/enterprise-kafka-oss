import urllib.request
import urllib.parse
import http.cookiejar
import re
import json

BASE_URL = "http://localhost:8080"
CLUSTER = "enterprise-kafka"

def create_opener():
    cj = http.cookiejar.CookieJar()
    return urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))

def login(username, password):
    opener = create_opener()
    req = urllib.request.Request(f"{BASE_URL}/oauth2/authorization/keycloak")
    with opener.open(req) as resp:
        html = resp.read().decode('utf-8')
        final_url = resp.geturl()

    m = re.search(r'action="([^"]+)"', html)
    if not m:
        print(f"[{username}] Form action not found")
        return None
    action_url = m.group(1).replace("&amp;", "&")
    if not action_url.startswith("http"):
        action_url = urllib.parse.urljoin(final_url, action_url)

    data = urllib.parse.urlencode({
        "username": username,
        "password": password
    }).encode('utf-8')
    req = urllib.request.Request(action_url, data=data, method="POST")
    with opener.open(req) as resp:
        pass
    return opener

def check_acl_access(opener, username):
    url = f"{BASE_URL}/api/clusters/{CLUSTER}/acls"
    req = urllib.request.Request(url)
    try:
        with opener.open(req) as resp:
            data = json.loads(resp.read().decode('utf-8'))
            print(f"[{username}] ACL Access: ALLOWED (HTTP 200, count={len(data)})")
            return "ALLOWED", len(data)
    except urllib.error.HTTPError as e:
        print(f"[{username}] ACL Access: BLOCKED (HTTP {e.code})")
        return f"BLOCKED_{e.code}", 0

print("--- Testing ACL Access Restriction ---")
admin_opener = login("admin", "admin123")
check_acl_access(admin_opener, "admin")

fi_opener = login("fi", "fi123")
check_acl_access(fi_opener, "fi")

eq_opener = login("equities", "equities123")
check_acl_access(eq_opener, "equities")
