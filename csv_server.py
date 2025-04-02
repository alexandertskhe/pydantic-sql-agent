import os
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from typing import Optional

# Create FastAPI app
app = FastAPI(title="CSV Export Server")

# Directory to store CSV exports
EXPORT_DIR = "csv_exports"
os.makedirs(EXPORT_DIR, exist_ok=True)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Mount the export directory as a static file directory
app.mount("/files", StaticFiles(directory=EXPORT_DIR), name="files")

@app.get("/")
async def root():
    """Root endpoint to check if the server is running."""
    return {"message": "CSV Export Server is running"}

@app.get("/download/{filename}")
async def download_file(filename: str):
    """Download a specific file with proper headers."""
    file_path = os.path.join(EXPORT_DIR, filename)
    
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
    
    # Return file with Content-Disposition header to force download
    return FileResponse(
        path=file_path, 
        filename=filename,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

if __name__ == "__main__":
    # Run the server on port 8000
    uvicorn.run(app, host="0.0.0.0", port=8000)