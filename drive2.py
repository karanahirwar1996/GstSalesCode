# -*- coding: utf-8 -*-
"""drive2.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1qgTvsqo55OqaIfq33T9lerp6_B51E-ee
"""

import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from tqdm import tqdm
import io
json_data_dict ={
        "type": "service_account",
        "project_id": "driveapi-396610",
        "private_key_id": "c75337f30c0870d8dd585496e02e8dbeb3cf229d",
        "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDqHxHtqSVbKi3X\nr93F4OARfEwerQB54mf8jJ3qO2axfNPPxwut7O72vrIa1FijaTRqvfCcB0aSi/IB\n4cKoL4GegwsBp/Wd+KiHkDoSXv/4N35zN4ZBqNujo4GV2ntgEdmsnIvhAY5NZXbH\nR0FGwItF5jj8PCB6PdI6hSS2zg0NKtySYwkhdHlJxCjHtLu1Rb9N2SGaTudrvLKV\n1r35thZuNbAzuLicuC0DukJT8pxhTbS1ST3RmpA7ug2zQSgkpK5iv0akpnjbDNyx\nz+ytNhWbFyNkzasKTDhd/hMg1RWSnDf98Q4plCbbAZheC/bbEJAi4pt24sWOJUV6\nzxNB8ZSrAgMBAAECggEAGGVGa/pdHyPFBR2ZQV5OWuQV1nh2fTzfUwygA+FOsR3t\nwE/gYq42tFVon60S02xJ/vlt0gRcETct745Dx1yz5/2FrxV+XYiknwOjWXi2uXmm\n3oChp8Pdpy6JeUD77CXQBdGGLdsIpf31o4xEPAgiOxVjSL1HMRWyC1EGY1oTOBTV\nQR1K/LQpQiP1XG/7IclpZZNZZNPjZKc3+WVDj/xcx15MC8swxaNGDx+o5afY0ul7\nApxxLOsCAjnafNV7Es7FnI2Nz4yg5Y1MRRMNey8UHPnmvDL6qa3W3YF4x6B145hp\nWzfogWyrW3a3LZXcRzGKSe+u3kkSyolcOD3AvT9pwQKBgQD5ygM1vnlme0h4v2Ih\nZmVIaY7J71CXj98+3BJhN4oHJFq+FIamxZZDIc8X4MrA9aTqMMZOULMw48v2CwVE\nYKwtUHC/FpystUKFyv1iGbb7UsQG++lM+b6L1cnW4rbc+aAGICmKo0bVPrC06Fco\nFbJGwvNTuF4pA6hDc40UPPibCwKBgQDv8VPJ3XIiWjotv+wN0Jl+iiSrQfsxNiYU\nJdVrq0UvWaJv41HWbfqtBqENiQ7MNbQwqrGnEJXXLmY/5IO3+DvQwdDIqoyvnPRx\nuoAfp2GW0BpKjX2B/X24s9OVKJdl17CfV8CzkhqbV8liwUds2jfOgulKJQKaYZ8h\nYfC5hKXw4QKBgHvnzlHRiyzfyKJE5SuGPIV//xmCQar87hOjXOamgyxpxy10xxpg\n9tmUIsNIearf7w9QZH4in9CHnvwMmW9CuQW9WkAfulYdj8MIX0pTUSY39w8z1JWf\naPq6cOXMDkNs/Akt2Q1xUsii0Urb2agDoyxgtgz4bpTPwJ686eV5HSTjAoGAB/Sr\nf4z9JNBzD2NGs2qQPFbeQmNsrcQK3S4n9mr2X0yMi0MxSnfZEPWgT2+U8wZw1BBE\n1bJCFaFvOH0eNPJhIVnbz1uAUK5WmJLDfskw/iwmQwSP/chm68HiqRZwdqsBKzdg\np1OX2EC/56ta7+wIX6uNiqzRekb0XMn/jlcsnWECgYEA6W1SlsOBIDYkefytsMRn\nETaiSK4Dr+HGwwTRzBmXkl1py9kxyJNQb6X2AKvQCPh9RIydQL9fb6l43kzZrmyF\n2pKHBXgXOhs+nwaYSkw0Lf261/YPbK/st9LTBqfRF6RMP6LJQQHXcgcxVRBnUiJG\nn21J0kxisrMo3dV4Rqzrfvw=\n-----END PRIVATE KEY-----\n",
        "client_email": "driveapi@driveapi-396610.iam.gserviceaccount.com",
        "client_id": "112352709061905236450",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/driveapi%40driveapi-396610.iam.gserviceaccount.com",
        "universe_domain": "googleapis.com"
    }

def initialize_drive_service(json_data_dict):
    creds = service_account.Credentials.from_service_account_info(json_data_dict)
    drive_service = build('drive', 'v3', credentials=creds)
    return drive_service

def download_and_parse_file(drive_service, file_id, file_name):
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    pbar = None

    while not done:
        status, done = downloader.next_chunk()
        if pbar is None:
            pbar = tqdm(total=100, desc="Progress", unit="%",
                        bar_format="{desc} {percentage:3.0f}%|{bar:20}|")
        if status:
            pbar.update(int(status.progress() * 100))

    pbar.close()
    fh.seek(0)

    try:
        if file_name.lower().endswith('.csv'):
            df = pd.read_csv(fh)
        elif file_name.lower().endswith('.xlsx'):
            df = pd.read_excel(fh)
        else:
            print(f"File {file_name} has an unsupported format. Skipping...")
            return None
    except pd.errors.ParserError:
        print(f"File {file_name} cannot be read. Skipping...")
        return None

    return df

def drive_libV2(folder_id):
    drive_service = initialize_drive_service(json_data_dict)
    results = drive_service.files().list(q=f"'{folder_id}' in parents",
                                         fields="nextPageToken, files(id, name)").execute()
    items = results.get('files', [])

    if not items:
        print('No files found. Kindly Shared Editor Request To - driveapi@driveapi-396610.iam.gserviceaccount.com')
        return None

    print('Files:')
    final_df = pd.DataFrame()

    for item in items:
        file_name = item['name']
        file_id = item['id']
        df = download_and_parse_file(drive_service, file_id, file_name)

        if df is not None:
            final_df = pd.concat([final_df, df])

    return final_df