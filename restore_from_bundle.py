
import os
import re

BUNDLE_FILE = "SAAS_TECHNICAL_BUNDLE.md"

def restore_from_bundle():
    if not os.path.exists(BUNDLE_FILE):
        print(f"Error: {BUNDLE_FILE} not found.")
        return

    print(f"Reading {BUNDLE_FILE}...")
    with open(BUNDLE_FILE, 'r', encoding='utf-8') as f:
        content = f.read()

    # Regex to find code blocks marked with ### File: <path>
    # Format matches:
    # ### File: `path/to/file`
    # ```language
    # content
    # ```
    pattern = re.compile(r"### File: `([^`]+)`\n```\w*\n(.*?)```", re.DOTALL)
    
    matches = pattern.findall(content)
    
    print(f"Found {len(matches)} files to restore.")
    
    for relative_path, file_content in matches:
        # Normalize path
        file_path = os.path.abspath(relative_path)
        
        # Security check: Ensure we stay within current directory
        if not file_path.startswith(os.getcwd()):
            print(f"Skipping potentially unsafe path: {relative_path}")
            continue
            
        # Create directories if needed
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # Write file
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(file_content)
            
        print(f"Restored: {relative_path}")

    print("Restoration complete.")

if __name__ == "__main__":
    restore_from_bundle()
