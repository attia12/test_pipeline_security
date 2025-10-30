package fr.tictak.dema.model;

import lombok.Data;

@Data
public class DriverMissionSummaryDTO {
    private String moveId;              // Unique identifier for the mission
    private String duration;            // Duration of the mission (e.g., "4h30")
    private double distanceKm;          // Distance in kilometers (e.g., 360.0)
    private String sourceAddress;       // Source address (e.g., "6391 Elgin St. Celina, Delaware 10299")
    private String destinationAddress;
    private int productCount;           // Number of products (e.g., 16)
    private double amount;              // Amount/cost (e.g., 2260.30)
    private String status;              // Mission status (e.g., "COMPLETED")
    private String driverFullName;
    private String driverPhoneNumber;
    private String date;
}