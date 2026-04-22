using System.Data;
using ConsoleApp5.DTOs;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;

namespace ConsoleApp5.Controllers;

[ApiController]
[Route("api/appointments")]
public class AppointmentsController : ControllerBase
{
    private static readonly HashSet<string> AllowedStatuses = new(StringComparer.Ordinal)
    {
        "Scheduled",
        "Completed",
        "Cancelled"
    };

    private readonly string _connectionString;

    public AppointmentsController(IConfiguration configuration)
    {
        _connectionString = configuration.GetConnectionString("DefaultConnection")
            ?? throw new InvalidOperationException("Connection string 'DefaultConnection' is not configured.");
    }

    [HttpGet]
    [ProducesResponseType(typeof(IEnumerable<AppointmentListDto>), StatusCodes.Status200OK)]
    public async Task<IActionResult> GetAll(
        [FromQuery] string? status,
        [FromQuery] string? patientLastName,
        CancellationToken cancellationToken)
    {
        var result = new List<AppointmentListDto>();

        const string sql = """
            SELECT
                a.IdAppointment,
                a.AppointmentDate,
                a.Status,
                a.Reason,
                p.FirstName + N' ' + p.LastName AS PatientFullName,
                p.Email AS PatientEmail
            FROM dbo.Appointments a
            JOIN dbo.Patients p ON p.IdPatient = a.IdPatient
            WHERE (@Status IS NULL OR a.Status = @Status)
              AND (@PatientLastName IS NULL OR p.LastName = @PatientLastName)
            ORDER BY a.AppointmentDate;
            """;

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = new SqlCommand(sql, connection);
        command.Parameters.Add("@Status", SqlDbType.NVarChar, 30).Value =
            string.IsNullOrWhiteSpace(status) ? DBNull.Value : status;
        command.Parameters.Add("@PatientLastName", SqlDbType.NVarChar, 80).Value =
            string.IsNullOrWhiteSpace(patientLastName) ? DBNull.Value : patientLastName;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(new AppointmentListDto
            {
                IdAppointment = reader.GetInt32(0),
                AppointmentDate = reader.GetDateTime(1),
                Status = reader.GetString(2),
                Reason = reader.GetString(3),
                PatientFullName = reader.GetString(4),
                PatientEmail = reader.GetString(5)
            });
        }

        return Ok(result);
    }

    [HttpGet("{idAppointment:int}")]
    [ProducesResponseType(typeof(AppointmentDetailsDto), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ErrorResponseDto), StatusCodes.Status404NotFound)]
    public async Task<IActionResult> GetById(int idAppointment, CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT
                a.IdAppointment,
                a.AppointmentDate,
                a.Status,
                a.Reason,
                a.InternalNotes,
                a.CreatedAt,
                p.IdPatient,
                p.FirstName,
                p.LastName,
                p.Email,
                p.PhoneNumber,
                d.IdDoctor,
                d.FirstName,
                d.LastName,
                d.LicenseNumber,
                s.Name AS SpecializationName
            FROM dbo.Appointments a
            JOIN dbo.Patients p ON p.IdPatient = a.IdPatient
            JOIN dbo.Doctors d ON d.IdDoctor = a.IdDoctor
            JOIN dbo.Specializations s ON s.IdSpecialization = d.IdSpecialization
            WHERE a.IdAppointment = @IdAppointment;
            """;

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = new SqlCommand(sql, connection);
        command.Parameters.Add("@IdAppointment", SqlDbType.Int).Value = idAppointment;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return NotFound(new ErrorResponseDto($"Appointment with id {idAppointment} was not found."));
        }

        var dto = new AppointmentDetailsDto
        {
            IdAppointment = reader.GetInt32(0),
            AppointmentDate = reader.GetDateTime(1),
            Status = reader.GetString(2),
            Reason = reader.GetString(3),
            InternalNotes = reader.IsDBNull(4) ? null : reader.GetString(4),
            CreatedAt = reader.GetDateTime(5),
            IdPatient = reader.GetInt32(6),
            PatientFirstName = reader.GetString(7),
            PatientLastName = reader.GetString(8),
            PatientEmail = reader.GetString(9),
            PatientPhoneNumber = reader.GetString(10),
            IdDoctor = reader.GetInt32(11),
            DoctorFirstName = reader.GetString(12),
            DoctorLastName = reader.GetString(13),
            DoctorLicenseNumber = reader.GetString(14),
            SpecializationName = reader.GetString(15)
        };

        return Ok(dto);
    }

    [HttpPost]
    [ProducesResponseType(typeof(AppointmentDetailsDto), StatusCodes.Status201Created)]
    [ProducesResponseType(typeof(ErrorResponseDto), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(ErrorResponseDto), StatusCodes.Status409Conflict)]
    public async Task<IActionResult> Create(
        [FromBody] CreateAppointmentRequestDto request,
        CancellationToken cancellationToken)
    {
        if (!ModelState.IsValid)
        {
            return BadRequest(new ErrorResponseDto("Invalid request payload."));
        }

        if (string.IsNullOrWhiteSpace(request.Reason))
        {
            return BadRequest(new ErrorResponseDto("Reason is required."));
        }

        if (request.Reason.Length > 250)
        {
            return BadRequest(new ErrorResponseDto("Reason cannot exceed 250 characters."));
        }

        if (request.AppointmentDate <= DateTime.UtcNow)
        {
            return BadRequest(new ErrorResponseDto("Appointment date cannot be in the past."));
        }

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        if (!await IsPatientActiveAsync(connection, request.IdPatient, cancellationToken))
        {
            return BadRequest(new ErrorResponseDto($"Patient with id {request.IdPatient} does not exist or is inactive."));
        }

        if (!await IsDoctorActiveAsync(connection, request.IdDoctor, cancellationToken))
        {
            return BadRequest(new ErrorResponseDto($"Doctor with id {request.IdDoctor} does not exist or is inactive."));
        }

        if (await HasDoctorConflictAsync(connection, request.IdDoctor, request.AppointmentDate, null, cancellationToken))
        {
            return Conflict(new ErrorResponseDto("The doctor already has an appointment scheduled at this date and time."));
        }

        const string insertSql = """
            INSERT INTO dbo.Appointments (IdPatient, IdDoctor, AppointmentDate, Status, Reason)
            OUTPUT INSERTED.IdAppointment
            VALUES (@IdPatient, @IdDoctor, @AppointmentDate, N'Scheduled', @Reason);
            """;

        await using var command = new SqlCommand(insertSql, connection);
        command.Parameters.Add("@IdPatient", SqlDbType.Int).Value = request.IdPatient;
        command.Parameters.Add("@IdDoctor", SqlDbType.Int).Value = request.IdDoctor;
        command.Parameters.Add("@AppointmentDate", SqlDbType.DateTime2).Value = request.AppointmentDate;
        command.Parameters.Add("@Reason", SqlDbType.NVarChar, 250).Value = request.Reason;

        var newIdObj = await command.ExecuteScalarAsync(cancellationToken);
        if (newIdObj is null)
        {
            return StatusCode(StatusCodes.Status500InternalServerError,
                new ErrorResponseDto("Failed to insert appointment."));
        }

        var newId = (int)newIdObj;

        return CreatedAtAction(nameof(GetById), new { idAppointment = newId }, new { IdAppointment = newId });
    }

    [HttpPut("{idAppointment:int}")]
    [ProducesResponseType(typeof(AppointmentDetailsDto), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ErrorResponseDto), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(ErrorResponseDto), StatusCodes.Status404NotFound)]
    [ProducesResponseType(typeof(ErrorResponseDto), StatusCodes.Status409Conflict)]
    public async Task<IActionResult> Update(
        int idAppointment,
        [FromBody] UpdateAppointmentRequestDto request,
        CancellationToken cancellationToken)
    {
        if (!ModelState.IsValid)
        {
            return BadRequest(new ErrorResponseDto("Invalid request payload."));
        }

        if (!AllowedStatuses.Contains(request.Status))
        {
            return BadRequest(new ErrorResponseDto("Status must be one of: Scheduled, Completed, Cancelled."));
        }

        if (string.IsNullOrWhiteSpace(request.Reason) || request.Reason.Length > 250)
        {
            return BadRequest(new ErrorResponseDto("Reason is required and cannot exceed 250 characters."));
        }

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var existing = await GetAppointmentStatusAndDateAsync(connection, idAppointment, cancellationToken);
        if (existing is null)
        {
            return NotFound(new ErrorResponseDto($"Appointment with id {idAppointment} was not found."));
        }

        var (currentStatus, currentDate) = existing.Value;

        if (currentStatus == "Completed" && request.AppointmentDate != currentDate)
        {
            return Conflict(new ErrorResponseDto("Cannot change the date of a completed appointment."));
        }

        if (!await IsPatientActiveAsync(connection, request.IdPatient, cancellationToken))
        {
            return BadRequest(new ErrorResponseDto($"Patient with id {request.IdPatient} does not exist or is inactive."));
        }

        if (!await IsDoctorActiveAsync(connection, request.IdDoctor, cancellationToken))
        {
            return BadRequest(new ErrorResponseDto($"Doctor with id {request.IdDoctor} does not exist or is inactive."));
        }

        if (request.AppointmentDate != currentDate)
        {
            if (await HasDoctorConflictAsync(connection, request.IdDoctor, request.AppointmentDate, idAppointment, cancellationToken))
            {
                return Conflict(new ErrorResponseDto("The doctor already has an appointment scheduled at this date and time."));
            }
        }

        const string updateSql = """
            UPDATE dbo.Appointments
            SET IdPatient = @IdPatient,
                IdDoctor = @IdDoctor,
                AppointmentDate = @AppointmentDate,
                Status = @Status,
                Reason = @Reason,
                InternalNotes = @InternalNotes
            WHERE IdAppointment = @IdAppointment;
            """;

        await using var command = new SqlCommand(updateSql, connection);
        command.Parameters.Add("@IdPatient", SqlDbType.Int).Value = request.IdPatient;
        command.Parameters.Add("@IdDoctor", SqlDbType.Int).Value = request.IdDoctor;
        command.Parameters.Add("@AppointmentDate", SqlDbType.DateTime2).Value = request.AppointmentDate;
        command.Parameters.Add("@Status", SqlDbType.NVarChar, 30).Value = request.Status;
        command.Parameters.Add("@Reason", SqlDbType.NVarChar, 250).Value = request.Reason;
        command.Parameters.Add("@InternalNotes", SqlDbType.NVarChar, 500).Value =
            string.IsNullOrWhiteSpace(request.InternalNotes) ? DBNull.Value : request.InternalNotes;
        command.Parameters.Add("@IdAppointment", SqlDbType.Int).Value = idAppointment;

        var affected = await command.ExecuteNonQueryAsync(cancellationToken);
        if (affected == 0)
        {
            return NotFound(new ErrorResponseDto($"Appointment with id {idAppointment} was not found."));
        }

        return await GetById(idAppointment, cancellationToken);
    }

    [HttpDelete("{idAppointment:int}")]
    [ProducesResponseType(StatusCodes.Status204NoContent)]
    [ProducesResponseType(typeof(ErrorResponseDto), StatusCodes.Status404NotFound)]
    [ProducesResponseType(typeof(ErrorResponseDto), StatusCodes.Status409Conflict)]
    public async Task<IActionResult> Delete(int idAppointment, CancellationToken cancellationToken)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var existing = await GetAppointmentStatusAndDateAsync(connection, idAppointment, cancellationToken);
        if (existing is null)
        {
            return NotFound(new ErrorResponseDto($"Appointment with id {idAppointment} was not found."));
        }

        var (currentStatus, _) = existing.Value;
        if (currentStatus == "Completed")
        {
            return Conflict(new ErrorResponseDto("Cannot delete a completed appointment."));
        }

        const string deleteSql = "DELETE FROM dbo.Appointments WHERE IdAppointment = @IdAppointment;";

        await using var command = new SqlCommand(deleteSql, connection);
        command.Parameters.Add("@IdAppointment", SqlDbType.Int).Value = idAppointment;

        var affected = await command.ExecuteNonQueryAsync(cancellationToken);
        if (affected == 0)
        {
            return NotFound(new ErrorResponseDto($"Appointment with id {idAppointment} was not found."));
        }

        return NoContent();
    }

    private static async Task<bool> IsPatientActiveAsync(
        SqlConnection connection,
        int idPatient,
        CancellationToken cancellationToken)
    {
        const string sql = "SELECT IsActive FROM dbo.Patients WHERE IdPatient = @IdPatient;";

        await using var command = new SqlCommand(sql, connection);
        command.Parameters.Add("@IdPatient", SqlDbType.Int).Value = idPatient;

        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is bool isActive && isActive;
    }

    private static async Task<bool> IsDoctorActiveAsync(
        SqlConnection connection,
        int idDoctor,
        CancellationToken cancellationToken)
    {
        const string sql = "SELECT IsActive FROM dbo.Doctors WHERE IdDoctor = @IdDoctor;";

        await using var command = new SqlCommand(sql, connection);
        command.Parameters.Add("@IdDoctor", SqlDbType.Int).Value = idDoctor;

        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is bool isActive && isActive;
    }

    private static async Task<bool> HasDoctorConflictAsync(
        SqlConnection connection,
        int idDoctor,
        DateTime appointmentDate,
        int? excludeIdAppointment,
        CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT COUNT(1)
            FROM dbo.Appointments
            WHERE IdDoctor = @IdDoctor
              AND AppointmentDate = @AppointmentDate
              AND Status <> N'Cancelled'
              AND (@ExcludeId IS NULL OR IdAppointment <> @ExcludeId);
            """;

        await using var command = new SqlCommand(sql, connection);
        command.Parameters.Add("@IdDoctor", SqlDbType.Int).Value = idDoctor;
        command.Parameters.Add("@AppointmentDate", SqlDbType.DateTime2).Value = appointmentDate;
        command.Parameters.Add("@ExcludeId", SqlDbType.Int).Value =
            excludeIdAppointment.HasValue ? excludeIdAppointment.Value : DBNull.Value;

        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is int count && count > 0;
    }

    private static async Task<(string Status, DateTime AppointmentDate)?> GetAppointmentStatusAndDateAsync(
        SqlConnection connection,
        int idAppointment,
        CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT Status, AppointmentDate
            FROM dbo.Appointments
            WHERE IdAppointment = @IdAppointment;
            """;

        await using var command = new SqlCommand(sql, connection);
        command.Parameters.Add("@IdAppointment", SqlDbType.Int).Value = idAppointment;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return (reader.GetString(0), reader.GetDateTime(1));
    }
}
