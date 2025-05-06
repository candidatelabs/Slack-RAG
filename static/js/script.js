document.addEventListener('DOMContentLoaded', function() {
    // Initialize date picker
    const dateRangePicker = flatpickr("#dateRange", {
        mode: "range",
        dateFormat: "Y-m-d",
        maxDate: "today",
        defaultDate: [new Date().setDate(new Date().getDate() - 7), new Date()],
    });

    // Handle form submission
    document.getElementById('digestForm').addEventListener('submit', async function(e) {
        e.preventDefault();
        
        const dates = dateRangePicker.selectedDates;
        if (dates.length !== 2) {
            alert('Please select both start and end dates');
            return;
        }

        const startDate = formatDate(dates[0]);
        const endDate = formatDate(dates[1]);

        // Show loading indicator
        document.getElementById('loading').style.display = 'block';
        document.getElementById('results').style.display = 'none';

        try {
            const response = await fetch('/generate', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    start_date: startDate,
                    end_date: endDate
                })
            });

            const data = await response.json();
            
            if (data.status === 'success') {
                displayResults(data.data);
                setupDownloadButton(data.csv_filename);
            } else {
                throw new Error(data.error || 'Failed to generate digest');
            }
        } catch (error) {
            alert('Error generating digest: ' + error.message);
        } finally {
            document.getElementById('loading').style.display = 'none';
        }
    });

    // Handle CSV download
    document.getElementById('downloadCsv').addEventListener('click', function() {
        const filename = this.getAttribute('data-filename');
        if (filename) {
            window.location.href = `/download/${filename}`;
        }
    });
});

function formatDate(date) {
    return date.toISOString().split('T')[0];
}

function displayResults(results) {
    const tbody = document.querySelector('#resultsTable tbody');
    tbody.innerHTML = '';

    results.forEach(result => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${result.candidate_name}</td>
            <td><a href="${result.linkedin_url}" target="_blank">${result.linkedin_url}</a></td>
            <td>${result.channel}</td>
            <td>${result.client}</td>
            <td>${new Date(result.submission_date * 1000).toLocaleDateString()}</td>
        `;
        tbody.appendChild(row);
    });

    document.getElementById('results').style.display = 'block';
}

function setupDownloadButton(filename) {
    const downloadBtn = document.getElementById('downloadCsv');
    downloadBtn.setAttribute('data-filename', filename);
} 