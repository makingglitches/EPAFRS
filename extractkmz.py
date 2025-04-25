from zipfile import ZipFile
import tempfile
from bs4 import BeautifulSoup
import os
import csv

def extract_kmz_data(kmz_file,output_dir, skipExtract=False):
   
   zf =  ZipFile(kmz_file)

   with tempfile.TemporaryDirectory() as tempdir:
       zf.extractall(tempdir)
       for dire in os.walk(tempdir):
           for f in dire[2]:
               if f.endswith('.KML'):                   
                on = f.replace('.KML','.CSV').lower()
                if not skipExtract:
                    print(f'extracting.. {f}')
                    extract_kml_data(os.path.join(tempdir,f),os.path.join(output_dir,on))
                else:
                    print(f'using existing... {f}')
                return os.path.join(output_dir,on)
    
def extract_kml_data(kml_file, output_csv)->str:
    tree = ET.parse(kml_file)
    root = tree.getroot()
    
    # Adjust namespace
    ns = {'kml': 'http://earth.google.com/kml/2.2'}
    
    data = []
    
    for epa_folder in root.findall(".//kml:Folder", ns):
        if epa_folder.find("kml:name", ns) is None or "EPA Facilities by State" not in epa_folder.find("kml:name", ns).text:
            continue
        
        for state_folder in epa_folder.findall(".//kml:Folder", ns):
            state_name = state_folder.find("kml:name", ns)
            state_text = state_name.text if state_name is not None else ""
            
            for city_folder in state_folder.findall(".//kml:Folder", ns):
                city_name = city_folder.find("kml:name", ns)
                city_text = city_name.text if city_name is not None else ""
                u
                for placemark in city_folder.findall('.//kml:Placemark', ns):
                    title = placemark.find('kml:name', ns)
                    coords = placemark.find('.//kml:coordinates', ns)
                    desc = placemark.find('kml:description', ns)
                    
                    title_text = title.text if title is not None else ""
                    coords_text = coords.text.strip() if coords is not None else ""
                    
                    iframe_link = ""
                    if desc is not None and desc.text:
                        soup = BeautifulSoup(desc.text, "html.parser")
                        iframe = soup.find("iframe")
                        if iframe and iframe.has_attr("src"):
                            iframe_link = iframe["src"]
                    
                    data.append([title_text, coords_text, state_text, city_text, iframe_link])
    
    # Write to CSV
    with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Title", "Coordinates", "State", "City", "Iframe Link"])
        writer.writerows(data)
    
    print(f"Extracted {len(data)} entries and saved to {output_csv}")

    return output_csv
