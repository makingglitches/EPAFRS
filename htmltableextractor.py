from bs4 import BeautifulSoup
import wgetwrapper as w  # Your own module, assumed working

# BeautifulSoup: because websites lie.
# (Also: "You didn’t write that awful page. You’re just trying to get data out of it.")

def extract_tables_with_bs4(html_string):
    soup = BeautifulSoup(html_string, "html.parser")
    tables = []

    for table in soup.find_all("table"):
        headers = []
        rows = []

        # Try to find header row (from <th>)
        header_row = table.find("tr")
        if header_row:
            ths = header_row.find_all("th")
            if ths:
                headers = [th.get_text(strip=True) for th in ths]

        # Go through each row
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if not tds:
                continue

            values = []
            for td in tds:
                # Include link if it's in the cell
                a = td.find("a")
                if a and a.has_attr("href"):
                    link = a["href"]
                    text = a.get_text(strip=True)
                    values.append(f"{text} ({link})")
                else:
                    values.append(td.get_text(strip=True))

            # Match with headers if possible
            if headers and len(headers) == len(values):
                row_dict = dict(zip(headers, values))
            else:
                row_dict = {f"col{i+1}": val for i, val in enumerate(values)}

            rows.append(row_dict)

        if rows:
            tables.append(rows)

    return tables


if __name__ == "__main__":
    html = w.fetch_html_with_wget("https://ofmpub.epa.gov/frs_public2/national_kml.registry_html?p_registry_id=110001406194")
    tables = extract_tables_with_bs4(html)
    for i, table in enumerate(tables):
        print(f"Table {i+1}:")
        for row in table:
            print(row)
