import os
import re
import time
import requests


def fetch_pdf_from_doi(doi, save_dir="downloaded_pdfs", email="dan@metascienceobservatory.org", delay=0.2):
    """
    Try to download a PDF for a DOI using multiple fallbacks:
      0. OSF  if identified as OSF DOI
      1. OpenAlex
      2. Unpaywall
      3. Crossref (direct PDF links or landing page)
      4. Europe PMC
      5. Semantic Scholar
      6. Direct DOI resolver (html scraping)

    Saves PDF to save_dir as: doi.replace('/', '--') + '.pdf'
    Returns the path if successful, else None.
    """
    if not isinstance(doi, str) or not doi.strip():
        return None

    doi = doi.strip()
    safe_filename = doi.replace("/", "--") + ".pdf"
    os.makedirs(save_dir, exist_ok=True)
    save_path = os.path.join(save_dir, safe_filename)

    # Skip if already downloaded
    if os.path.exists(save_path):
        print(f"📄 Already have {safe_filename}")
        return save_path

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0 Safari/537.36"
        )
    }

    def try_download(url):
        """Try downloading PDF from URL and save to save_path."""
        if not url:
            return False
        try:
            r = requests.get(url, headers=headers, timeout=25, allow_redirects=True)
            if r.status_code == 200 and "application/pdf" in r.headers.get("content-type", "").lower():
                with open(save_path, "wb") as f:
                    f.write(r.content)
                print(f"✅ Downloaded from {url}")
                return True
        except Exception:
            pass
        return False


    # ---------------- 0  OSF DOI handling ----------------
    try:
        if doi.lower().startswith("10.17605/osf.io") or "osf" in doi.lower():
            # Normalize DOI → OSF identifier
            osf_id = doi.split("/")[-1].replace("%2F", "").replace("OSF.IO", "").strip().lower()
            if not osf_id:
                osf_id = re.findall(r"osf\.io/([a-z0-9]+)", doi.lower())
                osf_id = osf_id[0] if osf_id else None

            if osf_id:
                # Try the simple direct download first
                candidate_urls = [
                    f"https://osf.io/{osf_id}/download",
                    f"https://osf.io/{osf_id}/?action=download",
                    f"https://osf.io/{osf_id}/",
                ]

                for url in candidate_urls:
                    if try_download(url):
                        print(f"✅ OSF direct download success for {doi}")
                        return save_path

                # Try the OSF API (to find attached files)
                r = requests.get(f"https://api.osf.io/v2/nodes/{osf_id}/files/", timeout=10)
                if r.status_code == 200:
                    files_json = r.json()
                    for entry in files_json.get("data", []):
                        links = entry.get("links", {})
                        pdf_url = links.get("download")
                        if pdf_url and pdf_url.lower().endswith(".pdf"):
                            if try_download(pdf_url):
                                print(f"✅ OSF API file download success for {doi}")
                                return save_path
    except Exception as e:
        print(f"⚠️ OSF download failed for {doi}: {e}")
        pass

    
    # ---------------- 1️⃣ OpenAlex ----------------
    try:
        r = requests.get(f"https://api.openalex.org/works/https://doi.org/{doi}", timeout=10)
        if r.status_code == 200:
            data = r.json()
            best = data.get("best_oa_location") or {}
            pdf_url = best.get("url_for_pdf") or best.get("url")
            if try_download(pdf_url):
                print(f"✅ OpenAlex success for {doi}")
                return save_path
    except Exception:
        pass
    time.sleep(delay)

    # ---------------- 2️⃣ Unpaywall ----------------
    try:
        r = requests.get(f"https://api.unpaywall.org/v2/{doi}?email={email}", timeout=10)
        if r.status_code == 200:
            data = r.json()
            best = data.get("best_oa_location") or {}
            pdf_url = best.get("url_for_pdf") or best.get("url")
            if try_download(pdf_url):
                print(f"✅ Unpaywall success for {doi}")
                return save_path
    except Exception:
        pass
    time.sleep(delay)

    # ---------------- 3️⃣ Crossref ----------------
    try:
        r = requests.get(f"https://api.crossref.org/works/{doi}", timeout=10)
        if r.status_code == 200:
            m = r.json().get("message", {})
            # Direct PDF links in Crossref metadata
            for link in m.get("link", []):
                if link.get("content-type") == "application/pdf":
                    if try_download(link.get("URL")):
                        print(f"✅ Crossref direct link success for {doi}")
                        return save_path
            # Landing page fallback
            landing = m.get("URL")
            if try_download(landing):
                print(f"✅ Crossref landing page worked for {doi}")
                return save_path
    except Exception:
        pass
    time.sleep(delay)

    # ---------------- 4️⃣ Europe PMC ----------------
    try:
        r = requests.get(
            f"https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=DOI:{doi}&format=json",
            timeout=10,
        )
        if r.status_code == 200:
            results = r.json().get("resultList", {}).get("result", [])
            if results:
                full_urls = results[0].get("fullTextUrlList", {}).get("fullTextUrl", [])
                for u in full_urls:
                    if "pdf" in (u.get("url", "").lower()):
                        if try_download(u["url"]):
                            print(f"✅ EuropePMC success for {doi}")
                            return save_path
    except Exception:
        pass
    time.sleep(delay)

    # ---------------- 5️⃣ Semantic Scholar ----------------
    try:
        r = requests.get(
            f"https://api.semanticscholar.org/graph/v1/paper/DOI:{doi}?fields=openAccessPdf",
            timeout=10,
        )
        if r.status_code == 200:
            pdf_url = r.json().get("openAccessPdf", {}).get("url")
            if try_download(pdf_url):
                print(f"✅ Semantic Scholar success for {doi}")
                return save_path
    except Exception:
        pass
    time.sleep(delay)

    # ---------------- 6️⃣ Direct DOI resolver ----------------
    try:
        resolved_url = f"https://doi.org/{doi}"
        r = requests.get(resolved_url, headers=headers, timeout=20, allow_redirects=True)
        if r.status_code == 200:
            # Direct PDF response
            if "application/pdf" in r.headers.get("content-type", "").lower():
                with open(save_path, "wb") as f:
                    f.write(r.content)
                print(f"✅ Direct DOI PDF success for {doi}")
                return save_path

            # Search HTML for .pdf links
            pdf_links = re.findall(r'href=["\'](.*?\.pdf)["\']', r.text, re.IGNORECASE)
            for link in pdf_links:
                if link.startswith("/"):
                    base = re.match(r"^https?://[^/]+", r.url)
                    if base:
                        link = base.group(0) + link
                if try_download(link):
                    print(f"✅ Found PDF via DOI HTML for {doi}")
                    return save_path
    except Exception:
        pass

       
    time.sleep(delay)


    
    print(f"❌ Could not fetch PDF for {doi}")
    return None
