using System.ComponentModel.DataAnnotations;

namespace ConsoleApp5.DTOs;

public class CreateAppointmentRequestDto
{
    [Required]
    public int IdPatient { get; set; }

    [Required]
    public int IdDoctor { get; set; }

    [Required]
    public DateTime AppointmentDate { get; set; }

    [Required]
    [StringLength(250, MinimumLength = 1)]
    public string Reason { get; set; } = string.Empty;
}
