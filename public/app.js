let currentJobId = null;
let statusInterval = null;
async function runPipeline() {

  const clientId = document.getElementById('clientId').value;
  const industry = document.getElementById('industry').value;
  const promptText = document.getElementById('promptText').value;

  if (!clientId || !industry || !promptText) {
    alert("Please fill all fields");
    return;
  }

  try {

    const response = await fetch('/run', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        clientId,
        industry,
        promptText
      })
    });

    const data = await response.json();

    currentJobId = data.jobId;
    if (statusInterval) {
  clearInterval(statusInterval);
}

statusInterval = setInterval(async () => {
  try {
    const response = await fetch(`/status/${currentJobId}`);
    const status = await response.json();

    document.getElementById('statusBox').innerText =
      JSON.stringify(status, null, 2);

    if (
      status.status === 'completed' ||
      status.status === 'failed'
    ) {
      clearInterval(statusInterval);
    }
  } catch (err) {
    clearInterval(statusInterval);
  }
}, 3000);

    document.getElementById('jobSection').style.display = 'block';
    document.getElementById('jobId').innerText = currentJobId;

    document.getElementById('statusBox').innerText =
      `Pipeline Started\nJob ID: ${currentJobId}`;

  } catch (err) {
    document.getElementById('statusBox').innerText =
      `Error: ${err.message}`;
  }
}

